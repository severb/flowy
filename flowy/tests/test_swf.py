import unittest
from mock import create_autospec, sentinel, Mock, call, ANY


class TestCaseNoLogging(unittest.TestCase):
    def setUp(self):
        import logging
        logging.root.disabled = True

    def tearDown(self):
        import logging
        logging.root.disabled = False


class SWFClientTest(TestCaseNoLogging):
    def _get_uut(self, domain='domain'):
        from flowy.swf import SWFClient
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

    def test_start_workflow(self):
        m, c = self._get_uut(domain='dom')
        m.start_workflow_execution.return_value = {'runId': 'run_id'}
        r = c.start_workflow(name='name', version=3, task_list='taskl',
                             input='data')
        self.assertEquals(r, 'run_id')
        m.start_workflow_execution.assert_called_once_with(
            domain='dom', workflow_id=ANY, workflow_name='name',
            workflow_version='3', task_list='taskl', input='data'
        )

    def test_start_workflow_error(self):
        m, c = self._get_uut(domain='dom')
        from boto.swf.exceptions import SWFResponseError
        m.start_workflow_execution.side_effect = SWFResponseError(0, 0)
        r = c.start_workflow(name='name', version=3, task_list='taskl',
                             input='data')
        self.assertEquals(r, None)

    def test_schedule(self):
        m, c = self._get_uut()
        c.queue_activity(call_id='call1', name='name', version=3, input='i')
        c.queue_activity(call_id='call2', name='name', version=3, input='i',
                         heartbeat=10, schedule_to_close=11, task_list='tl',
                         schedule_to_start=12, start_to_close=13, context='c')
        c.queue_subworkflow(workflow_id='w1', name='wfn', version=2, input='i')
        c.queue_subworkflow(workflow_id='w2', name='wfn', version=2, input='i',
                            task_start_to_close=1, execution_start_to_close=2,
                            task_list='tl1', context='c')
        c.queue_timer(call_id='t1', delay=10)
        c.queue_timer(call_id='t2', delay=10, context='c')
        r = c.schedule_queued(token='tok', context='ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_with(
            task_token='tok', decisions=[
                {'scheduleActivityTaskDecisionAttributes': {
                    'input': 'i',
                    'activityId': 'call1',
                    'activityType': {'version': '3', 'name': 'name'}},
                 'decisionType': 'ScheduleActivityTask'},
                {'scheduleActivityTaskDecisionAttributes': {
                    'taskList': {'name': 'tl'},
                    'scheduleToCloseTimeout': '11',
                    'activityType': {'version': '3', 'name': 'name'},
                    'control': 'c',
                    'heartbeatTimeout': '10',
                    'activityId': 'call2',
                    'scheduleToStartTimeout': '12',
                    'input': 'i',
                    'startToCloseTimeout': '13'},
                 'decisionType': 'ScheduleActivityTask'},
                {'startChildWorkflowExecutionDecisionAttributes': {
                    'input': 'i',
                    'workflowId': 'w1',
                    'workflowType': {'name': 'wfn', 'version': '2'}},
                 'decisionType': 'StartChildWorkflowExecution'},
                {'startChildWorkflowExecutionDecisionAttributes': {
                    'control': 'c',
                    'executionStartToCloseTimeout': 2,
                    'input': 'i',
                    'taskList': {'name': 'tl1'},
                    'taskStartToCloseTimeout': 1,
                    'workflowId': 'w2',
                    'workflowType': {'name': 'wfn', 'version': '2'}},
                 'decisionType': 'StartChildWorkflowExecution'},
                {'startTimerDecisionAttributes': {
                    'startToFireTimeout': '10',
                    'timerId': 't1'},
                 'decisionType': 'StartTimer'},
                {'startTimerDecisionAttributes': {
                    'control': 'c',
                    'startToFireTimeout': '10',
                    'timerId': 't2'},
                 'decisionType': 'StartTimer'}
            ], execution_context='ctx'
        )

    def test_schedule_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        err = SWFResponseError(0, 0)
        m.respond_decision_task_completed.side_effect = err
        c.queue_activity(call_id='call1', name='name', version=3, input='i')
        c.queue_activity(call_id='call2', name='name', version=3, input='i',
                         heartbeat=10, schedule_to_close=11, task_list='tl',
                         schedule_to_start=12, start_to_close=13)
        r = c.schedule_queued(token='tok', context='ctx')
        self.assertFalse(r)

    def test_complete_workflow(self):
        m, c = self._get_uut()
        self.assertTrue(c.complete_workflow(token='tok', result='r'))
        m.respond_decision_task_completed.assert_called_once_with(
            task_token='tok', decisions=[{
                'completeWorkflowExecutionDecisionAttributes': {
                    'result': 'r'
                },
                'decisionType': 'CompleteWorkflowExecution'
            }]
        )

    def test_complete_workflow_err(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_decision_task_completed.side_effect = SWFResponseError(0, 0)
        self.assertFalse(c.complete_workflow(token='tok', result='r'))

    def test_fail_workflow(self):
        m, c = self._get_uut()
        self.assertTrue(c.fail_workflow(token='tok', reason='r'))
        m.respond_decision_task_completed.assert_called_once_with(
            task_token='tok', decisions=[{
                'failWorkflowExecutionDecisionAttributes': {'reason': 'r'},
                'decisionType': 'FailWorkflowExecution'
            }]
        )

    def test_fail_workflow_err(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_decision_task_completed.side_effect = SWFResponseError(0, 0)
        self.assertFalse(c.fail_workflow(token='tok', reason='r'))

    def test_complete_activity(self):
        m, c = self._get_uut()
        self.assertTrue(c.complete_activity(token='tok', result='r'))
        m.respond_activity_task_completed.assert_called_once_with(
            task_token='tok', result='r'
        )

    def test_complete_activity_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_activity_task_completed.side_effect = SWFResponseError(0, 0)
        self.assertFalse(c.complete_activity(token='tok', result='r'))

    def test_fail_activity(self):
        m, c = self._get_uut()
        self.assertTrue(c.fail_activity(token='tok', reason='reason'))
        m.respond_activity_task_failed.assert_called_once_with(
            task_token='tok', reason='reason'
        )

    def test_fail_activity_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.respond_activity_task_failed.side_effect = SWFResponseError(0, 0)
        self.assertFalse(c.fail_activity(token='tok', reason='reason'))

    def test_heartbeat(self):
        m, c = self._get_uut()
        self.assertTrue(c.heartbeat(token='tok'))
        m.record_activity_task_heartbeat.assert_called_once_with(
            task_token='tok'
        )

    def test_heartbeat_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        m.record_activity_task_heartbeat.side_effect = SWFResponseError(0, 0)
        self.assertFalse(c.heartbeat(token='tok'))

    def test_poll_activity(self):
        m, c = self._get_uut(domain='dom')
        m.poll_for_activity_task.return_value = {
            'activityType': {'name': 'aname', 'version': 'aversion'},
            'input': 'ainput',
            'taskToken': 'atoken'
        }
        from flowy.swf import ActivityResponse
        self.assertEquals(
            c.poll_activity('task_list'),
            ActivityResponse(name='aname', version='aversion',
                             input='ainput', token='atoken')
        )
        m.poll_for_activity_task.assert_called_once_with(
            domain='dom', task_list='task_list'
        )

    def test_poll_decision_one_page(self):
        m, c = self._get_uut(domain='dom')
        m.poll_for_decision_task.return_value = {
            'taskToken': 'wtoken',
            'workflowType': {'name': 'wname', 'version': 'wversion'},
            'events': [sentinel.e1, sentinel.e2, sentinel.e3],
        }
        factory = Mock()
        factory.side_effect = [sentinel.r1, sentinel.r2, sentinel.r3]
        r = c.poll_decision('task_list', event_factory=factory)
        from flowy.swf import DecisionResponse
        m.poll_for_decision_task.assert_called_once_with(
            domain='dom', task_list='task_list', reverse_order=True
        )
        self.assertEquals(
            r,
            DecisionResponse(name='wname', version='wversion', token='wtoken',
                             last_event_id=None, events_iter=ANY)
        )
        self.assertEquals(
            list(r.events_iter),
            [sentinel.r1, sentinel.r2, sentinel.r3]
        )
        factory.assert_has_calls(
            [call(sentinel.e1), call(sentinel.e2), call(sentinel.e3)]
        )

    def test_poll_decision_empty_pages(self):
        m, c = self._get_uut(domain='dom')
        m.poll_for_decision_task.side_effect = {'taskToken': None}, {}, {}, {
            'taskToken': 'wtoken',
            'workflowType': {'name': 'wname', 'version': 'wversion'},
            'events': [],
        }
        r = c.poll_decision('task_list')
        from flowy.swf import DecisionResponse
        self.assertEquals(
            r,
            DecisionResponse(name='wname', version='wversion', token='wtoken',
                             last_event_id=None, events_iter=ANY)
        )

    def test_poll_decision_paginated(self):
        m, c = self._get_uut(domain='dom')
        s = sentinel
        m.poll_for_decision_task.side_effect = [
            {
                'taskToken': 'wtoken',
                'workflowType': {'name': 'wname', 'version': 'wversion'},
                'events': [s.e1, s.e2, s.e3],
                'nextPageToken': 'p1',
            },
            {
                'taskToken': 'wtoken',
                'workflowType': {'name': 'wname', 'version': 'wversion'},
                'events': [s.e4, s.e5, s.e6],
            }
        ]
        factory = Mock()
        factory.side_effect = [s.r1, s.r2, s.r3, s.r4, s.r5, s.r6]
        r = c.poll_decision('task_list', event_factory=factory)
        from flowy.swf import DecisionResponse
        self.assertEquals(
            r,
            DecisionResponse(name='wname', version='wversion', token='wtoken',
                             last_event_id=None, events_iter=ANY)
        )
        self.assertEquals(
            list(r.events_iter), [s.r1, s.r2, s.r3, s.r4, s.r5, s.r6]
        )
        m.poll_for_decision_task.assert_has_calls([
            call(domain='dom', task_list='task_list', reverse_order=True),
            call(domain='dom', task_list='task_list', reverse_order=True,
                 next_page_token='p1')
        ])

    def test_poll_decision_paginated_retries(self):
        m, c = self._get_uut(domain='dom')
        s = sentinel
        m.poll_for_decision_task.side_effect = [
            {
                'taskToken': 'wtoken',
                'workflowType': {'name': 'wname', 'version': 'wversion'},
                'events': [s.e1, s.e2, s.e3],
                'nextPageToken': 'p1',
            },
            {}, {}, {},
            {
                'taskToken': 'wtoken',
                'workflowType': {'name': 'wname', 'version': 'wversion'},
                'events': [s.e4, s.e5, s.e6],
            }
        ]
        factory = Mock()
        r = c.poll_decision('task_list', event_factory=factory, page_retry=3)
        self.assertEquals(len(list(r.events_iter)), 6)

    def test_poll_decision_paginated_fails(self):
        m, c = self._get_uut(domain='dom')
        s = sentinel
        m.poll_for_decision_task.side_effect = [
            {
                'taskToken': 'wtoken',
                'workflowType': {'name': 'wname', 'version': 'wversion'},
                'events': [s.e1, s.e2, s.e3],
                'nextPageToken': 'p1',
            },
            {}, {}, {}, {}
        ]
        factory = Mock()
        r = c.poll_decision('task_list', event_factory=factory, page_retry=3)
        from flowy.swf import PageError
        self.assertRaises(PageError, list, r.events_iter)


class ActivityTaskTest(unittest.TestCase):
    def _get_uut(self, token='token'):
        from flowy.swf import ActivityTask, SWFClient
        client = create_autospec(SWFClient, instance=True)
        return client, ActivityTask(client, token=token)

    def test_complete(self):
        c, at = self._get_uut(token='tok')
        c.complete_activity.return_value = sentinel.r
        self.assertEquals(at.complete(result='r'), sentinel.r)
        c.complete_activity.assert_called_once_with(token='tok', result='r')

    def test_fail(self):
        c, at = self._get_uut(token='tok')
        c.fail_activity.return_value = sentinel.r
        self.assertEquals(at.fail(reason='r'), sentinel.r)
        c.fail_activity.assert_called_once_with(token='tok', reason='r')

    def test_heartbeat(self):
        c, at = self._get_uut(token='tok')
        c.heartbeat.return_value = sentinel.r
        self.assertEquals(at.heartbeat(), sentinel.r)
        c.heartbeat.assert_called_once_with(token='tok')
