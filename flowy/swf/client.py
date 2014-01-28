from boto.swf.exceptions import SWFResponseError

from flowy.runtime import OptionsRuntime
from flowy.swf import SWFTaskId
from flowy.swf.runtime import ActivityRuntime, DecisionRuntime


class _PaginationError(RuntimeError):
    """ A page of the history is unavailable. """


class ActivityPollerClient(object):
    def __init__(self, client, task_list, runtime_factory=ActivityRuntime):
        self._client = client
        self._task_list = task_list
        self._runtime_factory = runtime_factory

    def poll_next_task(self, poller):
        task = None
        while task is None:
            swf_response = self._poll_response()
            task_id, input, token = self._parse_response(swf_response)
            runtime = self._runtime_factory(
                client=self._client, token=token
            )
            task = poller.make_task(
                task_id=task_id,
                input=input,
                runtime=runtime
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
                pass  # Add a delay before retrying?
        return swf_response


def decision_runtime(client, events):
    return OptionsRuntime(DecisionRuntime(client, events))


class DecisionPollerClient(object):
    def __init__(self, client, task_list, runtime_factory=decision_runtime):
        self._client = client
        self._task_list = task_list
        self._runtime_factory = runtime_factory

    def poll_next_task(self, poller):
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
            runtime = self._runtime_factory(
                client=self._client,
                token=token,
                running=running,
                timedout=timedout,
                results=results,
                errors=errors
            )
            task = poller.make_task(
                task_id=task_id,
                input=input,
                runtime=runtime
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
            assert (
                next_p['taskToken'] == page['taskToken']
                and (
                    next_p['workflowType']['name']
                    == page['workflowType']['name'])
                and (
                    next_p['workflowType']['version']
                    == page['workflowType']['version'])
                and (
                    next_p.get('previousStartedEventId')
                    == page.get('previousStartedEventId'))
            ), 'Inconsistent decision pages.'
            page = next_p

    def _parse_events(self, events):
        running, timedout, results, errors = set(), set(), {}, {}
        event2call = {}
        for e in events:
            e_type = e.get('eventType')
            # Activities
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
            elif e_type == 'StartChildWorkflowExecutionFailed':
                SCWEFEA = 'startChildWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(e[SCWEFEA]['workflowId'])
                reason = e[SCWEIEA]['cause']
                errors[id] = reason
            elif e_type == 'ChildWorkflowExecutionTimedOut':
                CWETOEA = 'childWorkflowExecutionTimedOutEventAttributes'
                id = _subworkflow_id(
                    e[CWETOEA]['workflowExecution']['workflowId']
                )
                running.remove(id)
                timedout.add(id)
            elif e_type == 'TimerStarted':
                id = e['timerStartedEventAttributes']['timerId']
                self.running.add(id)
            elif e_type == 'TimerFired':
                id = e['timerStartedEventAttributes']['timerId']
                running.remove(id)
                results[id] = None
        return running, timedout, results, errors

    def _poll_response_first_page(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._client.poll_for_decision_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                pass
        return swf_response

    def _poll_response_page(self, page_token):
        swf_response = None
        for _ in range(7):  # give up after a limited number of retries
            try:
                swf_response = self._client.poll_for_activity_task(
                    task_list=self._task_list, next_page_token=page_token
                )
                break
            except SWFResponseError:
                pass
        else:
            raise _PaginationError()
        return swf_response


def _subworkflow_id(workflow_id):
    return workflow_id.rsplit('-', 1)[-1]
