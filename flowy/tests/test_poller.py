import unittest
from copy import deepcopy

from mock import Mock


def swf_error():
    from boto.swf.exceptions import SWFResponseError
    return SWFResponseError(0, 0)


class TestSWFActivityPoller(unittest.TestCase):
    valid_response = {
        'activityType': {'name': 'at', 'version': 'v1'},
        'input': 'in',
        'taskToken': 'token1'
    }

    def _get_uut(self):
        from flowy.poller import SWFActivityPoller
        client, tf = Mock(), Mock()
        return (
            SWFActivityPoller(swf_client=client, task_list='tl',
                              task_factory=tf),
            client, tf
        )

    def test_valid_response(self):
        from flowy.spec import SWFSpecKey
        uut, client, factory = self._get_uut()
        client.poll_for_activity_task.return_value = self.valid_response
        result = uut.poll_next_task()
        client.poll_for_activity_task.assert_called_once_with(task_list='tl')
        factory.assert_called_once_with(
            SWFSpecKey('at', 'v1'),
            swf_client=client,
            input='in',
            token='token1'
        )
        self.assertEquals(result, factory())

    def test_ingnore_temp_errors(self):
        from flowy.spec import SWFSpecKey
        uut, client, factory = self._get_uut()
        client.poll_for_activity_task.side_effect = (
            swf_error(), swf_error(), self.valid_response
        )
        result = uut.poll_next_task()
        factory.assert_called_once_with(
            SWFSpecKey('at', 'v1'),
            swf_client=client,
            input='in',
            token='token1'
        )
        self.assertEquals(result, factory())

    def test_ignore_empty_responses(self):
        from flowy.spec import SWFSpecKey
        uut, client, factory = self._get_uut()
        client.poll_for_activity_task.side_effect = {}, {}, self.valid_response
        result = uut.poll_next_task()
        factory.assert_called_once_with(
            SWFSpecKey('at', 'v1'),
            swf_client=client,
            input='in',
            token='token1'
        )
        self.assertEquals(result, factory())

    def test_ignore_empty_task_tokens(self):
        from flowy.spec import SWFSpecKey
        uut, client, factory = self._get_uut()
        r = {'taskToken': ''}
        client.poll_for_activity_task.side_effect = r, r, self.valid_response
        result = uut.poll_next_task()
        factory.assert_called_once_with(
            SWFSpecKey('at', 'v1'),
            swf_client=client,
            input='in',
            token='token1'
        )
        self.assertEquals(result, factory())


class TestSWFWorkflowPoller(unittest.TestCase):
    no_events_response = {
        'events': [{
            'eventType': 'WorkflowExecutionStarted',
            'workflowExecutionStartedEventAttributes': {
                'input': 'in',
                'workflowType': {
                    'name': 'n',
                    'version': 'v1'
                },
                'taskList': {'name': 'tl'},
                'taskStartToCloseTimeout': '60',
                'executionStartToCloseTimeout': 120
            }
        }],
        'workflowType': {'name': 'n', 'version': 'v1'},
        'taskToken': 'token1'
    }

    def _get_uut(self):
        from flowy.poller import SWFWorkflowPoller
        client, task_factory, spec_factory = Mock(), Mock(), Mock()
        return (
            SWFWorkflowPoller(swf_client=client, task_list='tl',
                              task_factory=task_factory,
                              spec_factory=spec_factory),
            client, task_factory, spec_factory
        )

    def test_activity_events_parsing(self):
        from flowy.spec import SWFSpecKey

        response = deepcopy(self.no_events_response)
        response['events'].extend([
            {
                'eventType': 'ActivityTaskScheduled',
                'activityTaskScheduledEventAttributes': {'activityId': '1'},
                'eventId': 'e1'
            },
            {
                'eventType': 'ActivityTaskScheduled',
                'activityTaskScheduledEventAttributes': {'activityId': '2'},
                'eventId': 'e2'
            },
            {
                'eventType': 'ActivityTaskScheduled',
                'activityTaskScheduledEventAttributes': {'activityId': '3'},
                'eventId': 'e3'
            },
            {
                'eventType': 'ActivityTaskScheduled',
                'activityTaskScheduledEventAttributes': {'activityId': '4'},
                'eventId': 'e4'
            },
            {
                'eventType': 'ActivityTaskCompleted',
                'activityTaskCompletedEventAttributes': {
                    'scheduledEventId': 'e2', 'result': 'result'
                }
            },
            {
                'eventType': 'ActivityTaskFailed',
                'activityTaskFailedEventAttributes': {
                    'scheduledEventId': 'e3', 'reason': 'reason'
                }
            },
            {
                'eventType': 'ActivityTaskTimedOut',
                'activityTaskTimedOutEventAttributes': {
                    'scheduledEventId': 'e4'
                }
            },
            {
                'eventType': 'ScheduleActivityTaskFailed',
                'scheduleActivityTaskFailedEventAttributes': {
                    'activityId': '0', 'cause': 'cause'
                }
            }
        ])
        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.return_value = response
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['1']),
            set(['4']),
            {'2': 'result'},
            {'0': 'cause', '3': 'reason'},
            spec,
            None
        )
        self.assertEquals(result, tf())

    def test_workflow_event_parsing(self):
        from flowy.spec import SWFSpecKey

        response = deepcopy(self.no_events_response)
        response['events'] = list(self.no_events_response['events'])
        response['events'].extend([
            {
                'eventType': 'StartChildWorkflowExecutionInitiated',
                'startChildWorkflowExecutionInitiatedEventAttributes': {
                    'workflowId': 'a-1'
                },
            },
            {
                'eventType': 'StartChildWorkflowExecutionInitiated',
                'startChildWorkflowExecutionInitiatedEventAttributes': {
                    'workflowId': 'a-2'
                },
            },
            {
                'eventType': 'StartChildWorkflowExecutionInitiated',
                'startChildWorkflowExecutionInitiatedEventAttributes': {
                    'workflowId': 'a-3'
                },
            },
            {
                'eventType': 'StartChildWorkflowExecutionInitiated',
                'startChildWorkflowExecutionInitiatedEventAttributes': {
                    'workflowId': 'a-4'
                },
            },
            {
                'eventType': 'ChildWorkflowExecutionCompleted',
                'childWorkflowExecutionCompletedEventAttributes': {
                    'workflowExecution': {
                        'workflowId': 'a-2'
                    },
                    'result': 'result'
                }
            },
            {
                'eventType': 'ChildWorkflowExecutionFailed',
                'childWorkflowExecutionFailedEventAttributes': {
                    'workflowExecution': {
                        'workflowId': 'a-3'
                    },
                    'reason': 'reason'
                }
            },
            {
                'eventType': 'ChildWorkflowExecutionTimedOut',
                'childWorkflowExecutionTimedOutEventAttributes': {
                    'workflowExecution': {
                        'workflowId': 'a-4'
                    },
                }
            },
            {
                'eventType': 'StartChildWorkflowExecutionFailed',
                'startChildWorkflowExecutionFailedEventAttributes': {
                    'workflowId': 'a-0', 'cause': 'cause'
                }
            }
        ])
        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.return_value = response
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['1']),
            set(['4']),
            {'2': 'result'},
            {'0': 'cause', '3': 'reason'},
            spec,
            None
        )
        self.assertEquals(result, tf())

    def test_timer_event_parsing(self):
        from flowy.spec import SWFSpecKey

        response = deepcopy(self.no_events_response)
        response['events'] = list(self.no_events_response['events'])
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '2'}
            },
            {
                'eventType': 'TimerFired',
                'timerFiredEventAttributes': {'timerId': '1'}
            }
        ])
        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.return_value = response
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['2']),
            set([]),
            {'1': None},
            {},
            spec,
            None
        )
        self.assertEquals(result, tf())

    def test_multiple_pages(self):
        from flowy.spec import SWFSpecKey

        response = deepcopy(self.no_events_response)
        response['nextPageToken'] = 'whatever'
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
        ])
        response2 = deepcopy(self.no_events_response)
        response2['nextPageToken'] = 'whatever2'
        response2['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '2'}
            },
        ])
        response3 = deepcopy(self.no_events_response)
        response3['events'].extend([
            {
                'eventType': 'TimerFired',
                'timerFiredEventAttributes': {'timerId': '1'}
            },
        ])
        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            response,
            response2,
            response3
        ]
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['2']),
            set([]),
            {'1': None},
            {},
            spec,
            None
        )
        self.assertEquals(result, tf())

    def test_first_page_error(self):
        from flowy.spec import SWFSpecKey
        from boto.swf.exceptions import SWFResponseError

        response = deepcopy(self.no_events_response)
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
        ])
        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            SWFResponseError("Error", "Something"),
            response
        ]
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['1']),
            set([]),
            {},
            {},
            spec,
            None
        )
        self.assertEquals(result, tf())

    def test_other_pages_one_error(self):
        from flowy.spec import SWFSpecKey
        from boto.swf.exceptions import SWFResponseError

        response = deepcopy(self.no_events_response)
        response['nextPageToken'] = 'whatever'
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
        ])
        response2 = deepcopy(self.no_events_response)
        response2['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '2'}
            },
        ])

        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            response,
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            response2
        ]
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['1', '2']),
            set([]),
            {},
            {},
            spec,
            None
        )
        self.assertEquals(result, tf())

    def test_other_pages_time_out(self):
        from flowy.spec import SWFSpecKey
        from boto.swf.exceptions import SWFResponseError

        response = deepcopy(self.no_events_response)
        response['nextPageToken'] = 'whatever'
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
        ])

        uut, client, tf, sf = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            response,
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            response
        ]
        result = uut.poll_next_task()
        spec = sf('n', 'v1', 'tl', '60', '120')
        tf.assert_called_once_with(
            spec,
            client,
            'in',
            'token1',
            set(['1']),
            set([]),
            {},
            {},
            spec,
            None
        )
        self.assertEquals(result, tf())
