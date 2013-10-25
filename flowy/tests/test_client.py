import unittest
from mock import create_autospec


class SWFClientTest(unittest.TestCase):

    def setUp(self):
        import logging
        logging.root.disabled = True

    def tearDown(self):
        import logging
        logging.root.disabled = False

    def _get_uut(self, domain='domain'):
        from flowy.client import SWFClient
        from boto.swf.layer1 import Layer1
        m = create_autospec(Layer1, instance=True)
        return m, SWFClient(domain, client=m)

    def test_workflow_registration(self):
        m, c = self._get_uut(domain='dom')
        r = c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                descr='description')
        self.assertTrue(r)
        m.register_workflow_type.assert_called_once_with(
            domain='dom', name='name', version='3', task_list='taskl',
            default_child_policy='TERMINATE',
            default_execution_start_to_close_timeout='12',
            default_task_start_to_close_timeout='13',
            description='description'
        )

    def test_workflow_already_registered(self):
        m, c = self._get_uut(domain='dom')
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        m.register_workflow_type.side_effect = SWFTypeAlreadyExistsError(0, 0)
        m.describe_workflow_type.return_value = {
            'configuration': {
                'defaultExecutionStartToCloseTimeout': '12',
                'defaultTaskStartToCloseTimeout': '13',
                'defaultTaskList': {'name': 'taskl'},
                'defaultChildPolicy': 'TERMINATE'
            }
        }
        r = c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                descr='description')
        m.describe_workflow_type.assert_called_once_with(
            domain='dom', workflow_name='name', workflow_version='3'
        )
        self.assertTrue(r)

    def test_workflow_registration_bad_defaults(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        m.register_workflow_type.side_effect = SWFTypeAlreadyExistsError(0, 0)
        m.describe_workflow_type.return_value = {
            'configuration': {
                'defaultExecutionStartToCloseTimeout': '12',
                'defaultTaskStartToCloseTimeout': '13',
                'defaultTaskList': {'name': 'taskl'},
                'defaultChildPolicy': 'TERMINATE'
            }
        }
        self.assertFalse(
            c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=1,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                descr='description')
        )
        self.assertFalse(
            c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=12,
                                task_start_to_close=1,
                                child_policy='TERMINATE',
                                descr='description')
        )
        self.assertFalse(
            c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=12,
                                task_start_to_close=1,
                                child_policy='BADPOLICY',
                                descr='description')
        )
        self.assertFalse(
            c.register_workflow(name='name', version=3, task_list='badlist',
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                descr='description')
        )

    def test_workflow_registration_unknown_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.register_workflow_type.side_effect = SWFResponseError(0, 0)
        r = c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                descr='description')
        self.assertFalse(r)

    def test_workflow_registration_defaults_check_fails(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        from boto.swf.exceptions import SWFResponseError
        m.register_workflow_type.side_effect = SWFTypeAlreadyExistsError(0, 0)
        m.describe_workflow_type.side_effect = SWFResponseError(0, 0)
        r = c.register_workflow(name='name', version=3, task_list='taskl',
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                descr='description')
        self.assertFalse(r)


    def test_activity_registration(self):
        m, c = self._get_uut(domain='dom')
        r = c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        self.assertTrue(r)
        m.register_activity_type.assert_called_once_with(
            domain='dom', name='name', version='3', task_list='taskl',
            default_task_heartbeat_timeout='12',
            default_task_schedule_to_close_timeout='13',
            default_task_schedule_to_start_timeout='14',
            default_task_start_to_close_timeout='15',
            description='description'
        )

    def test_activity_already_registered(self):
        m, c = self._get_uut(domain='dom')
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        m.register_activity_type.side_effect = SWFTypeAlreadyExistsError(0, 0)
        m.describe_activity_type.return_value = {
            'configuration': {
                'defaultTaskStartToCloseTimeout': '15',
                'defaultTaskScheduleToStartTimeout': '14',
                'defaultTaskScheduleToCloseTimeout': '13',
                'defaultTaskHeartbeatTimeout': '12',
                'defaultTaskList': {'name': 'taskl'}
            }
        }
        r = c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        m.describe_activity_type.assert_called_once_with(
            domain='dom', activity_name='name', activity_version='3'
        )
        self.assertTrue(r)

    def test_activity_registration_bad_defaults(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        m.register_activity_type.side_effect = SWFTypeAlreadyExistsError(0, 0)
        m.describe_activity_type.return_value = {
            'configuration': {
                'defaultTaskStartToCloseTimeout': '15',
                'defaultTaskScheduleToStartTimeout': '14',
                'defaultTaskScheduleToCloseTimeout': '13',
                'defaultTaskHeartbeatTimeout': '12',
                'defaultTaskList': {'name': 'taskl'}
            }
        }
        self.assertFalse(
            c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=1, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        )
        self.assertFalse(
            c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=1,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        )
        self.assertFalse(
            c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=1, start_to_close=15,
                                descr='description')
        )
        self.assertFalse(
            c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=1,
                                descr='description')
        )
        self.assertFalse(
            c.register_activity(name='name', version=3, task_list='badlist',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        )

    def test_activity_registration_unknown_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.register_activity_type.side_effect = SWFResponseError(0, 0)
        r = c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        self.assertFalse(r)

    def test_activity_registration_defaults_check_fails(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        from boto.swf.exceptions import SWFResponseError
        m.register_activity_type.side_effect = SWFTypeAlreadyExistsError(0, 0)
        m.describe_activity_type.side_effect = SWFResponseError(0, 0)
        r = c.register_activity(name='name', version=3, task_list='taskl',
                                heartbeat=12, schedule_to_close=13,
                                schedule_to_start=14, start_to_close=15,
                                descr='description')
        self.assertFalse(r)

    def test_poll_activity(self):
        m, c = self._get_uut('dom')

        m.poll_for_activity_task.return_value = {
            'activityType': {'name': 'activity_name', 'version': 'version1'},
            'input': 'input_data',
            'taskToken': 'token'
        }

        from flowy.client import ActivityTask
        mock_ActivityTask = create_autospec(ActivityTask)

        c.poll_activity('taskl', activity_factory=mock_ActivityTask)

        m.poll_for_activity_task.assert_called_once_with(
            domain='dom', task_list='taskl'
        )
        mock_ActivityTask.assert_called_once_with(
            name='activity_name', version='version1', input='input_data',
            token='token', client=m
        )

    def test_poll_activity_skip_bad_responses(self):
        m, c = self._get_uut('dom')

        valid_response = {
            'activityType': {'name': '', 'version': ''},
            'input': '',
            'taskToken': 'token'
        }
        from boto.swf.exceptions import SWFResponseError
        error_response = SWFResponseError(0, 0)
        empty_response = {}
        m.poll_for_activity_task.side_effect = (
            [error_response] * 5
            + [empty_response] * 5
            + [error_response] * 5
            + [empty_response] * 5
            + [valid_response]
        )
        from flowy.client import ActivityTask
        mock_ActivityTask = create_autospec(ActivityTask)

        c.poll_activity('taskl', activity_factory=mock_ActivityTask)
        self.assertEquals(m.poll_for_activity_task.call_count, 21)
        self.assertEquals(mock_ActivityTask.call_count, 1)

    def test_poll_decision_first_run(self):
        m, c = self._get_uut('dom')

        m.poll_for_decision_task.return_value = {
            'workflowType': {'name': 'workflow_name', 'version': 'version1'},
            'taskToken': 'token',
            'events': [{
                'eventType': 'WorkflowExecutionStarted',
                'workflowExecutionStartedEventAttributes': {
                    'input': 'input_data'
                },
            }]
        }
        from flowy.client import Decision
        mock_Decision = create_autospec(Decision)

        c.poll_decision('taskl', decision_factory=mock_Decision)

        m.poll_for_decision_task.assert_called_once_with(
            domain='dom', task_list='taskl',
            next_page_token=None, reverse_order=True
        )
        mock_Decision.assert_called_once_with(
            name='workflow_name', version='version1', token='token',
            client=m, context=None, input='input_data', new_events=tuple()
        )

    def test_poll_decision_first_run_errors(self):
        m, c = self._get_uut('dom')

        valid_response = {
            'workflowType': {'name': '', 'version': ''},
            'taskToken': 'token',
            'events': [{
                'eventType': 'WorkflowExecutionStarted',
                'workflowExecutionStartedEventAttributes': {
                    'input': ''
                },
            }]
        }
        from boto.swf.exceptions import SWFResponseError
        error_response = SWFResponseError(0, 0)
        empty_response = {}
        m.poll_for_decision_task.side_effect = (
            [error_response] * 5
            + [empty_response] * 5
            + [error_response] * 5
            + [empty_response] * 5
            + [valid_response]
        )
        from flowy.client import Decision
        mock_Decision = create_autospec(Decision)

        c.poll_decision('taskl', decision_factory=mock_Decision)
        self.assertEquals(m.poll_for_decision_task.call_count, 21)
        self.assertEquals(mock_Decision.call_count, 1)

    def test_poll_decision_n_run(self):
        m, c = self._get_uut('dom')

        m.poll_for_decision_task.return_value = {
            'workflowType': {'name': '', 'version': ''},
            'taskToken': 'token',
            'previousStartedEventId': 50,
            'events': [
                {
                    'eventType': 'ActivityTaskScheduled',
                    'eventId': 100,
                    'activityTaskScheduledEventAttributes': {'activityId': 10},
                },
                {
                    'eventType': 'ActivityTaskCompleted',
                    'activityTaskCompletedEventAttributes': {
                        'scheduledEventId': 30, 'result': '30result',
                    },
                },
                {
                    'eventType': 'ActivityTaskFailed',
                    'activityTaskFailedEventAttributes': {
                        'scheduledEventId': 29, 'reason': '29reason',
                    },
                },
                {
                    'eventType': 'ActivityTaskTimedOut',
                    'activityTaskTimedOutEventAttributes': {
                        'scheduledEventId': 28,
                    },
                },
                {
                    'eventType': 'DecisionTaskCompleted',
                    'eventId': 50, 'executionContext': 'context',
                },
                {
                    'eventType': 'ActivityTaskTimedOut',
                    'activityTaskTimedOutEventAttributes': {
                        'scheduledEventId': 27,
                    },
                },
            ]
        }
        from flowy.client import Decision
        mock_Decision = create_autospec(Decision)
        c.poll_decision('taskl', decision_factory=mock_Decision)

        m.poll_for_decision_task.assert_called_once_with(
            domain='dom', task_list='taskl',
            next_page_token=None, reverse_order=True
        )
        mock_Decision.assert_called_once_with(
            name='', version='', token='token',
            client=m, context='context', input=None, new_events=(
                (28,), (29, '29reason'), (30, '30result'), (100, 10)
            )
        )

    def test_poll_decision_pagination(self):
        m, c = self._get_uut('dom')
        from boto.swf.exceptions import SWFResponseError
        error_response = SWFResponseError(0, 0)
        empty_response = {}
        m.poll_for_decision_task.side_effect = [
            empty_response,
            error_response,
            {
                'workflowType': {'name': '', 'version': ''},
                'taskToken': 'token',
                'previousStartedEventId': 50,
                'nextPageToken': 'page1',
                'events': [
                    {
                        'eventType': 'ActivityTaskScheduled',
                        'eventId': 100,
                        'activityTaskScheduledEventAttributes': {
                            'activityId': 10
                        },
                    },
                    {
                        'eventType': 'ActivityTaskCompleted',
                        'activityTaskCompletedEventAttributes': {
                            'scheduledEventId': 30, 'result': '30result',
                        },
                    },
                ]
            },
            empty_response,
            error_response,
            {
                'workflowType': {'name': '', 'version': ''},
                'taskToken': 'token',
                'nextPageToken': 'page2',
                'previousStartedEventId': 50,
                'events': [
                    {
                        'eventType': 'ActivityTaskCompleted',
                        'activityTaskCompletedEventAttributes': {
                            'scheduledEventId': 29, 'result': '29result',
                        },
                    },
                    {
                        'eventType': 'ActivityTaskCompleted',
                        'activityTaskCompletedEventAttributes': {
                            'scheduledEventId': 28, 'result': '28result',
                        },
                    },
                ]
            },
            empty_response,
            error_response,
            {
                'workflowType': {'name': '', 'version': ''},
                'taskToken': 'token',
                'previousStartedEventId': 50,
                'events': [
                    {
                        'eventType': 'ActivityTaskCompleted',
                        'activityTaskCompletedEventAttributes': {
                            'scheduledEventId': 27, 'result': '27result',
                        },
                    },
                    {
                        'eventType': 'DecisionTaskCompleted',
                        'eventId': 50, 'executionContext': 'context',
                    },
                    {
                        'eventType': 'ActivityTaskTimedOut',
                        'activityTaskTimedOutEventAttributes': {
                            'scheduledEventId': 27,
                        },
                    },
                ]
            },
        ]
        from flowy.client import Decision
        mock_Decision = create_autospec(Decision)

        c.poll_decision('taskl', decision_factory=mock_Decision)

        mock_Decision.assert_called_once_with(
            name='', version='', token='token',
            client=m, context='context', input=None, new_events=(
                (27, '27result'), (28, '28result'), (29, '29result'),
                (30, '30result'), (100, 10)
            )
        )
