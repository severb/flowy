import logging

from boto.swf.exceptions import SWFResponseError
from flowy.spec import SWFSpecKey, SWFWorkflowSpec


logger = logging.getLogger(__name__)


class SWFActivityPoller(object):
    def __init__(self, swf_client, task_list, task_factory):
        self._swf_client = swf_client
        self._task_list = task_list
        self._task_factory = task_factory

    def poll_next_task(self):
        swf_response = self._poll_response()
        spec_key, input, token = self._parse_response(swf_response)
        return self._task_factory(
            spec_key,
            swf_client=self._swf_client,
            input=input,
            token=token
        )

    def _parse_response(self, swf_response):
        return (
            SWFSpecKey(
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
                swf_response = self._swf_client.poll_for_activity_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                # add a delay before retrying?
                logger.exception('Error while polling for activities:')
        return swf_response


class SWFWorkflowPoller(object):
    def __init__(self, swf_client, task_list, task_factory,
                 spec_factory=SWFWorkflowSpec):
        self._swf_client = swf_client
        self._task_list = task_list
        self._task_factory = task_factory
        self._spec_factory = spec_factory

    def poll_next_task(self):
        first_page = self._poll_response_first_page()
        token = _parse_token(first_page)
        all_events = self._events(first_page)
        # the first page sometimes contains an empty events list, because
        # of that we can't get the WorkflowExecutionStarted before the
        # events generator is created - is this an Amazon SWF bug?
        first_event = next(all_events)
        input = _parse_input(first_event)
        spec = _parse_spec(first_event, self._spec_factory)
        tags = _parse_tags(first_event)
        try:
            p = self._parse_events
            running, timedout, results, errors, order = p(all_events)
        except _PaginationError:
            return self.poll_next_task()
        return self._task_factory(spec, self._swf_client, input, token,
                                  running, timedout, results, errors, order,
                                  spec, tags)

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
        running, timedout, results, errors, order = set(), set(), {}, {}, []
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
                order.append(id)
            elif e_type == 'ActivityTaskFailed':
                ATFEA = 'activityTaskFailedEventAttributes'
                id = event2call[e[ATFEA]['scheduledEventId']]
                reason = e[ATFEA]['reason']
                running.remove(id)
                errors[id] = reason
                order.append(id)
            elif e_type == 'ActivityTaskTimedOut':
                ATTOEA = 'activityTaskTimedOutEventAttributes'
                id = event2call[e[ATTOEA]['scheduledEventId']]
                running.remove(id)
                timedout.add(id)
                order.append(id)
            elif e_type == 'ScheduleActivityTaskFailed':
                SATFEA = 'scheduleActivityTaskFailedEventAttributes'
                id = e[SATFEA]['activityId']
                reason = e[SATFEA]['cause']
                # when a job is not found it's not even started
                errors[id] = reason
                order.append(id)
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
                order.append(id)
            elif e_type == 'ChildWorkflowExecutionFailed':
                CWEFEA = 'childWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(
                    e[CWEFEA]['workflowExecution']['workflowId']
                )
                reason = e[CWEFEA]['reason']
                running.remove(id)
                errors[id] = reason
                order.append(id)
            elif e_type == 'ChildWorkflowExecutionTimedOut':
                CWETOEA = 'childWorkflowExecutionTimedOutEventAttributes'
                id = _subworkflow_id(
                    e[CWETOEA]['workflowExecution']['workflowId']
                )
                running.remove(id)
                timedout.add(id)
                order.append(id)
            elif e_type == 'StartChildWorkflowExecutionFailed':
                SCWEFEA = 'startChildWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(e[SCWEFEA]['workflowId'])
                reason = e[SCWEFEA]['cause']
                errors[id] = reason
                order.append(id)
            elif e_type == 'TimerStarted':
                id = e['timerStartedEventAttributes']['timerId']
                running.add(id)
            elif e_type == 'TimerFired':
                id = e['timerFiredEventAttributes']['timerId']
                running.remove(id)
                results[id] = None
        return running, timedout, results, errors, order

    def _poll_response_first_page(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._swf_client.poll_for_decision_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                logger.exception('Error while polling for decisions:')
        return swf_response

    def _poll_response_page(self, page_token):
        swf_response = None
        for _ in range(7):  # give up after a limited number of retries
            try:
                swf_response = self._swf_client.poll_for_decision_task(
                    task_list=self._task_list, next_page_token=page_token)
                break
            except SWFResponseError:
                logger.exception('Error while polling for decision page:')
        else:
            raise _PaginationError()
        return swf_response


def _parse_token(page):
    return page['taskToken']


def _parse_input(event):
    assert event['eventType'] == 'WorkflowExecutionStarted'
    event_attrs = event['workflowExecutionStartedEventAttributes']
    return event_attrs['input']


def _parse_spec(event, factory):
    assert event['eventType'] == 'WorkflowExecutionStarted'
    event_attrs = event['workflowExecutionStartedEventAttributes']
    return factory(
        event_attrs['workflowType']['name'],
        event_attrs['workflowType']['version'],
        event_attrs['taskList']['name'],
        event_attrs['taskStartToCloseTimeout'],
        event_attrs['executionStartToCloseTimeout']
    )


def _parse_tags(event):
    assert event['eventType'] == 'WorkflowExecutionStarted'
    event_attrs = event['workflowExecutionStartedEventAttributes']
    return event_attrs.get('tagList', None)


def _subworkflow_id(workflow_id):
    return workflow_id.rsplit('-', 1)[-1]


class _PaginationError(RuntimeError):
    """ A page of the history is unavailable. """
