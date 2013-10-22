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

    def test_empty_scheduling(self):
        m, c = self._get_uut()
        r = c.schedule_activities('token', 'ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_once_with(
            task_token='token', decisions=[], execution_context='ctx'
        )

    a1 = {
        'scheduleActivityTaskDecisionAttributes': {
            'taskList': {'name': 'tl1'},
            'scheduleToCloseTimeout': '11',
            'activityType': {'version': '1', 'name': 'name1'},
            'heartbeatTimeout': '10',
            'activityId': 'call1',
            'scheduleToStartTimeout': '12',
            'input': 'input1',
            'startToCloseTimeout': '13'
        },
        'decisionType': 'ScheduleActivityTask'
    }
    a2 = {
        'scheduleActivityTaskDecisionAttributes': {
            'taskList': {'name': 'tl2'},
            'scheduleToCloseTimeout': '21',
            'activityType': {'version': '2', 'name': 'name2'},
            'heartbeatTimeout': '20',
            'activityId': 'call2',
            'scheduleToStartTimeout': '22',
            'input': 'input2',
            'startToCloseTimeout': '23'
        },
        'decisionType': 'ScheduleActivityTask'
    }

    def test_scheduling(self):
        m, c = self._get_uut()
        c.queue_activity('call1', 'name1', 1, 'input1', 10, 11, 12, 13, 'tl1')
        c.queue_activity('call2', 'name2', 2, 'input2', 20, 21, 22, 23, 'tl2')
        r = c.schedule_activities('token', 'ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_once_with(
            task_token='token', decisions=[self.a1, self.a2],
            execution_context='ctx'
        )

    def test_scheduling_defaults(self):
        m, c = self._get_uut()
        c.queue_activity('call1', 'name1', 1, 'input1')
        r = c.schedule_activities('token', 'ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_once_with(
            task_token='token', decisions=[{
                'scheduleActivityTaskDecisionAttributes': {
                    'input': 'input1',
                    'activityId': 'call1',
                    'activityType': {'version': '1', 'name': 'name1'}
                },
                'decisionType': 'ScheduleActivityTask'
            }],
            execution_context='ctx'
        )

    def test_error_when_scheduling(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_decision_task_completed.side_effect = SWFResponseError(0, 0)
        r = c.schedule_activities('token', 'ctx')
        self.assertFalse(r)

    def test_internal_list_on_scheduling_success(self):
        m, c = self._get_uut()
        c.queue_activity('call1', 'name1', 1, 'input1', 10, 11, 12, 13, 'tl1')
        c.queue_activity('call2', 'name2', 2, 'input2', 20, 21, 22, 23, 'tl2')
        c.schedule_activities('token', 'ctx')
        r = c.schedule_activities('token', 'ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_with(
            task_token='token', decisions=[], execution_context='ctx'
        )

    def test_internal_list_on_scheduling_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_decision_task_completed.side_effect = SWFResponseError(0, 0)
        c.queue_activity('call1', 'name1', 1, 'input1', 10, 11, 12, 13, 'tl1')
        c.queue_activity('call2', 'name2', 2, 'input2', 20, 21, 22, 23, 'tl2')
        c.schedule_activities('token', 'ctx')
        m.respond_decision_task_completed.side_effect = None
        r = c.schedule_activities('token', 'ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_with(
            task_token='token', decisions=[self.a1, self.a2],
            execution_context='ctx'
        )

    def test_workflow_complete(self):
        m, c = self._get_uut()
        r = c.complete_workflow('token', 'result')
        self.assertTrue(r)
        d = {
            'completeWorkflowExecutionDecisionAttributes': {
                'result': 'result'
            },
            'decisionType': 'CompleteWorkflowExecution'
        }
        m.respond_decision_task_completed.assert_called_once_with(
            task_token='token', decisions=[d]
        )

    def test_workflow_complete_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_decision_task_completed.side_effect = SWFResponseError(0, 0)
        r = c.complete_workflow('token', 'result')
        self.assertFalse(r)

    def test_workflow_terminate(self):
        m, c = self._get_uut(domain='dom')
        r = c.terminate_workflow('workflow_id', 'reason')
        self.assertTrue(r)
        m.terminate_workflow_execution.assert_called_once_with(
            domain='dom', workflow_id='workflow_id', reason='reason'
        )

    def test_workflow_terminate_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.terminate_workflow_execution.side_effect = SWFResponseError(0, 0)
        r = c.terminate_workflow('workflow_id', 'reason')
        self.assertFalse(r)

    def test_poll_decision(self):
        m, c = self._get_uut(domain='dom')
        m.poll_for_decision_task.return_value = 'decision'
        r = c.poll_decision('taskl')
        self.assertEquals(r, 'decision')
        m.poll_for_decision_task.assert_called_once_with(
            domain='dom', task_list='taskl',
            next_page_token=None, reverse_order=True,
        )
        r = c.poll_decision('taskl', next_page_token='nextpage')
        m.poll_for_decision_task.assert_called_with(
            domain='dom', task_list='taskl',
            next_page_token='nextpage', reverse_order=True,
        )

    def test_poll_decision_skip_errors(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        err = SWFResponseError(0, 0)
        m.poll_for_decision_task.side_effect = [err] * 5 + ['decision']
        r = c.poll_decision('taskl')
        self.assertEquals(r, 'decision')

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

    def test_activity_complete(self):
        m, c = self._get_uut()
        r = c.complete_activity('token', 'result')
        self.assertTrue(r)
        m.respond_activity_task_completed.assert_called_once_with(
            task_token='token', result='result'
        )

    def test_activity_complete_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_activity_task_completed.side_effect = SWFResponseError(0, 0)
        r = c.complete_activity('token', 'result')
        self.assertFalse(r)

    def test_activity_terminate(self):
        m, c = self._get_uut()
        r = c.terminate_activity('token', 'reason')
        self.assertTrue(r)
        m.respond_activity_task_failed.assert_called_once_with(
            task_token='token', reason='reason'
        )

    def test_activity_terminate_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_activity_task_failed.side_effect = SWFResponseError(0, 0)
        r = c.terminate_activity('token', 'reason')
        self.assertFalse(r)

    def test_activity_heartbeat(self):
        m, c = self._get_uut()
        r = c.heartbeat('token')
        self.assertTrue(r)
        m.record_activity_task_heartbeat.assert_called_once_with(
            task_token='token'
        )

    def test_activity_heartbeat_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.record_activity_task_heartbeat.side_effect = SWFResponseError(0, 0)
        r = c.heartbeat('token')
        self.assertFalse(r)

    def test_poll_activity(self):
        m, c = self._get_uut(domain='dom')
        m.poll_for_activity_task.return_value = 'activity'
        r = c.poll_activity('taskl')
        self.assertEquals(r, 'activity')
        m.poll_for_activity_task.assert_called_once_with(
            domain='dom', task_list='taskl',
        )

    def test_poll_activity_skip_errors(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        err = SWFResponseError(0, 0)
        m.poll_for_activity_task.side_effect = [err] * 5 + ['activity']
        r = c.poll_activity('taskl')
        self.assertEquals(r, 'activity')
