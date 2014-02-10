from boto.swf.exceptions import SWFResponseError

from flowy import logger
from flowy.scheduler import ArgsDependencyScheduler, OptionsScheduler
from flowy.swf import SWFTaskId
from flowy.swf.scheduler import ActivityScheduler, DecisionScheduler


class _PaginationError(RuntimeError):
    """ A page of the history is unavailable. """


class ActivityPoller(object):
    def __init__(self, client, task_list, scheduler=ActivityScheduler):
        self._client = client
        self._task_list = task_list
        self._scheduler = scheduler

    def poll_next_task(self, worker):
        task = None
        while task is None:
            swf_response = self._poll_response()
            task_id, input, token = self._parse_response(swf_response)
            scheduler = self._scheduler(
                client=self._client, token=token
            )
            task = worker.make_task(
                task_id=task_id,
                input=input,
                scheduler=scheduler
            )
        return task

    def _parse_response(self, swf_response):
        return (
            SWFTaskId(
                swf_response['activityType']['name'],
                swf_response['activityType']['version']
            ),
            swf_response['input'],
            swf_response['taskToken']
        )

    def _poll_response(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._client.poll_for_activity_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                # add a delay before retrying?
                logger.exception('Error while polling for activities:')
        return swf_response


def decision_scheduler(client, token, running, timedout, results, errors):
    return OptionsScheduler(
        ArgsDependencyScheduler(
            DecisionScheduler(
                client=client,
                token=token,
                running=running,
                timedout=timedout,
                results=results,
                errors=errors
            )
        )
    )


class DecisionPoller(object):
    def __init__(self, client, task_list, scheduler=decision_scheduler):
        self._client = client
        self._task_list = task_list
        self._scheduler = scheduler

    def poll_next_task(self, worker):
        task = None
        while task is None:
            first_page = self._poll_response_first_page()
            task_id = SWFTaskId(
                name=first_page['workflowType']['name'],
                version=first_page['workflowType']['version']
            )
            token = first_page['taskToken']
            all_events = self._events(first_page)
            # the first page sometimes contains an empty events list, because
            # of that we can't get the WorkflowExecutionStarted before the
            # events generator is created - is this an Amazon SWF bug?
            wes = all_events.next()
            assert wes['eventType'] == 'WorkflowExecutionStarted'
            input = wes['workflowExecutionStartedEventAttributes']['input']
            try:
                running, timedout, results, errors = self._parse_events(
                    all_events
                )
            except _PaginationError:
                continue
            scheduler = self._scheduler(
                client=self._client,
                token=token,
                running=running,
                timedout=timedout,
                results=results,
                errors=errors
            )
            task = worker.make_task(
                task_id=task_id,
                input=input,
                scheduler=scheduler
            )
        return task

    def _events(self, first_page):
        page = first_page
        while 1:
            for event in page['events']:
                yield event
            if not page.get('nextPageToken'):
                break
            next_p = self._poll_response_page(page_token=page['nextPageToken'])
            # curiously enough, this assert doesn't always hold...
            # assert (
            #     next_p['taskToken'] == page['taskToken']
            #     and (
            #         next_p['workflowType']['name']
            #         == page['workflowType']['name'])
            #     and (
            #         next_p['workflowType']['version']
            #         == page['workflowType']['version'])
            #     and (
            #         next_p.get('previousStartedEventId')
            #         == page.get('previousStartedEventId'))
            # ), 'Inconsistent decision pages.'
            page = next_p

    def _parse_events(self, events):
        running, timedout, results, errors = set(), set(), {}, {}
        event2call = {}
        for e in events:
            e_type = e.get('eventType')
            if e_type == 'ActivityTaskScheduled':
                id = e['activityTaskScheduledEventAttributes']['activityId']
                event2call[e['eventId']] = id
                running.add(id)
            elif e_type == 'ActivityTaskCompleted':
                ATCEA = 'activityTaskCompletedEventAttributes'
                id = event2call[e[ATCEA]['scheduledEventId']]
                result = e[ATCEA]['result']
                running.remove(id)
                results[id] = result
            elif e_type == 'ActivityTaskFailed':
                ATFEA = 'activityTaskFailedEventAttributes'
                id = event2call[e[ATFEA]['scheduledEventId']]
                reason = e[ATFEA]['reason']
                running.remove(id)
                errors[id] = reason
            elif e_type == 'ActivityTaskTimedOut':
                ATTOEA = 'activityTaskTimedOutEventAttributes'
                id = event2call[e[ATTOEA]['scheduledEventId']]
                running.remove(id)
                timedout.add(id)
            elif e_type == 'ScheduleActivityTaskFailed':
                SATFEA = 'scheduleActivityTaskFailedEventAttributes'
                id = e[SATFEA]['activityId']
                reason = e[SATFEA]['cause']
                # when a job is not found it's not even started
                errors[id] = reason
            elif e_type == 'StartChildWorkflowExecutionInitiated':
                SCWEIEA = 'startChildWorkflowExecutionInitiatedEventAttributes'
                id = _subworkflow_id(e[SCWEIEA]['workflowId'])
                running.add(id)
            elif e_type == 'ChildWorkflowExecutionCompleted':
                CWECEA = 'childWorkflowExecutionCompletedEventAttributes'
                id = _subworkflow_id(
                    e[CWECEA]['workflowExecution']['workflowId']
                )
                result = e[CWECEA]['result']
                running.remove(id)
                results[id] = result
            elif e_type == 'ChildWorkflowExecutionFailed':
                CWEFEA = 'childWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(
                    e[CWEFEA]['workflowExecution']['workflowId']
                )
                reason = e[CWEFEA]['reason']
                running.remove(id)
                errors[id] = reason
            elif e_type == 'ChildWorkflowExecutionTimedOut':
                CWETOEA = 'childWorkflowExecutionTimedOutEventAttributes'
                id = _subworkflow_id(
                    e[CWETOEA]['workflowExecution']['workflowId']
                )
                running.remove(id)
                timedout.add(id)
            elif e_type == 'StartChildWorkflowExecutionFailed':
                SCWEFEA = 'startChildWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(e[SCWEFEA]['workflowId'])
                reason = e[SCWEFEA]['cause']
                errors[id] = reason
            elif e_type == 'TimerStarted':
                id = e['timerStartedEventAttributes']['timerId']
                running.add(id)
            elif e_type == 'TimerFired':
                id = e['timerFiredEventAttributes']['timerId']
                running.remove(id)
                results[id] = None
        running = set(map(int, running))
        timedout = set(map(int, timedout))
        results = dict((int(k), v) for k, v in results.items())
        errors = dict((int(k), v) for k, v in errors.items())
        return running, timedout, results, errors

    def _poll_response_first_page(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._client.poll_for_decision_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                logger.exception('Error while polling for decisions:')
        return swf_response

    def _poll_response_page(self, page_token):
        swf_response = None
        for _ in range(7):  # give up after a limited number of retries
            try:
                swf_response = self._client.poll_for_decision_task(
                    task_list=self._task_list, next_page_token=page_token
                )
                break
            except SWFResponseError:
                logger.exception('Error while polling for decision page:')
        else:
            raise _PaginationError()
        return swf_response


def _subworkflow_id(workflow_id):
    return workflow_id.rsplit('-', 1)[-1]
