import unittest
from copy import deepcopy

from mock import Mock


def swf_error():
    from boto.swf.exceptions import SWFResponseError
    return SWFResponseError(0, 0)


class TestActivityPoller(unittest.TestCase):
    valid_response = {
        'activityType': {'name': 'at', 'version': 'v1'},
        'input': 'in',
        'taskToken': 'token1'
    }

    def _get_uut(self):
        from flowy.swf.poller import ActivityPoller
        client, scheduler = Mock(), Mock()
        return (
            ActivityPoller(client=client, task_list='tl', scheduler=scheduler),
            client, scheduler
        )

    def test_valid_response(self):
        from flowy.swf import SWFTaskId
        uut, client, scheduler = self._get_uut()
        client.poll_for_activity_task.return_value = self.valid_response
        worker = Mock()
        result = uut.poll_next_task(worker)
        client.poll_for_activity_task.assert_called_once_with(task_list='tl')
        scheduler.assert_called_once_with(client=client, token='token1')
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('at', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_ingnore_temp_errors(self):
        from flowy.swf import SWFTaskId
        uut, client, scheduler = self._get_uut()
        client.poll_for_activity_task.side_effect = (
            swf_error(), swf_error(), self.valid_response
        )
        worker = Mock()
        result = uut.poll_next_task(worker)
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('at', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_ignore_empty_responses(self):
        from flowy.swf import SWFTaskId
        uut, client, scheduler = self._get_uut()
        client.poll_for_activity_task.side_effect = {}, {}, self.valid_response
        worker = Mock()
        result = uut.poll_next_task(worker)
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('at', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_ignore_empty_task_tokens(self):
        from flowy.swf import SWFTaskId
        uut, client, scheduler = self._get_uut()
        r = {'taskToken': ''}
        client.poll_for_activity_task.side_effect = r, r, self.valid_response
        worker = Mock()
        result = uut.poll_next_task(worker)
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('at', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())


class TestDecisionPoller(unittest.TestCase):
    no_events_response = {
        'events': [{
            'eventType': 'WorkflowExecutionStarted',
            'workflowExecutionStartedEventAttributes': {'input': 'in'}
        }],
        'workflowType': {'name': 'n', 'version': 'v1'},
        'taskToken': 'token1'
    }

    def _get_uut(self):
        from flowy.swf.poller import DecisionPoller
        client, scheduler = Mock(), Mock()
        return (
            DecisionPoller(client=client, task_list='tl', scheduler=scheduler),
            client, scheduler
        )

    def test_activity_events_parsing(self):
        from flowy.swf import SWFTaskId

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
        uut, client, scheduler = self._get_uut()
        client.poll_for_decision_task.return_value = response
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([1]),
            timedout=set([4]),
            results={2: 'result'},
            errors={3: 'reason', 0: 'cause'},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_workflow_event_parsing(self):
        from flowy.swf import SWFTaskId

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
        uut, client, scheduler = self._get_uut()
        client.poll_for_decision_task.return_value = response
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([1]),
            timedout=set([4]),
            results={2: 'result'},
            errors={3: 'reason', 0: 'cause'},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_timer_event_parsing(self):
        from flowy.swf import SWFTaskId

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
        uut, client, scheduler = self._get_uut()
        client.poll_for_decision_task.return_value = response
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([2]),
            timedout=set(),
            results={1: None},
            errors={},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_multiple_pages(self):
        from flowy.swf import SWFTaskId

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
        uut, client, scheduler = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            response,
            response2,
            response3
        ]
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([2]),
            timedout=set(),
            results={1: None},
            errors={},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_first_page_error(self):
        from flowy.swf import SWFTaskId
        from boto.swf.exceptions import SWFResponseError

        response = deepcopy(self.no_events_response)
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
        ])
        uut, client, scheduler = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            SWFResponseError("Error", "Something"),
            response
        ]
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([1]),
            timedout=set(),
            results={},
            errors={},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_other_pages_one_error(self):
        from flowy.swf import SWFTaskId
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

        uut, client, scheduler = self._get_uut()
        client.poll_for_decision_task.side_effect = [
            response,
            SWFResponseError("Error", "Something"),
            SWFResponseError("Error", "Something"),
            response2
        ]
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([1, 2]),
            timedout=set(),
            results={},
            errors={},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())

    def test_other_pages_time_out(self):
        from flowy.swf import SWFTaskId
        from boto.swf.exceptions import SWFResponseError

        response = deepcopy(self.no_events_response)
        response['nextPageToken'] = 'whatever'
        response['events'].extend([
            {
                'eventType': 'TimerStarted',
                'timerStartedEventAttributes': {'timerId': '1'}
            },
        ])

        uut, client, scheduler = self._get_uut()
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
        worker = Mock()
        result = uut.poll_next_task(worker)
        scheduler.assert_called_once_with(
            client=client,
            token='token1',
            running=set([1]),
            timedout=set(),
            results={},
            errors={},
        )
        worker.make_task.assert_called_once_with(
            task_id=SWFTaskId('n', 'v1'),
            input='in',
            scheduler=scheduler()
        )
        self.assertEquals(result, worker.make_task())


class DecisionSchedulerTest(unittest.TestCase):

    def test_decision_scheduler(self):
        from flowy.swf.poller import decision_scheduler
        from flowy.scheduler import OptionsScheduler
        from flowy.scheduler import ArgsDependencyScheduler

        d_s = decision_scheduler(None, 'token', set(), set(), {}, {})

        self.assertIsInstance(d_s, OptionsScheduler)
        self.assertIsInstance(d_s._scheduler, ArgsDependencyScheduler)
