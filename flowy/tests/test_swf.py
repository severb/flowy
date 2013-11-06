import unittest
from mock import create_autospec, sentinel, Mock, call, ANY


class TestCaseNoLogging(unittest.TestCase):
    def setUp(self):
        import logging
        logging.root.disabled = True

    def tearDown(self):
        import logging
        logging.root.disabled = False


class DecisionPollingTest(TestCaseNoLogging):
    def assertTypedEquals(self, first, second, msg=None):
        self.assertEquals(first, second)
        self.assertEquals(type(first), type(second))

    def test_decision_event_activity_scheduled(self):
        from flowy.swf import _decision_event, _ActivityScheduled
        event = _decision_event({
            'eventType': 'ActivityTaskScheduled',
            'eventId': 'event_id',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'call_id',
            },
        })
        self.assertTypedEquals(event, _ActivityScheduled(event_id='event_id',
                                                         call_id='call_id'))

    def test_decision_event_acitivity_completed(self):
        from flowy.swf import _decision_event, _ActivityCompleted
        event = _decision_event({
            'eventType': 'ActivityTaskCompleted',
            'activityTaskCompletedEventAttributes': {
                'scheduledEventId': 'event_id',
                'result': 'res',
            },
        })
        self.assertTypedEquals(event, _ActivityCompleted(event_id='event_id',
                                                         result='res'))

    def test_decision_event_acitivity_failed(self):
        from flowy.swf import _decision_event, _ActivityFailed
        event = _decision_event({
            'eventType': 'ActivityTaskFailed',
            'activityTaskFailedEventAttributes': {
                'scheduledEventId': 'event_id',
                'reason': 'reas',
            },
        })
        self.assertTypedEquals(event, _ActivityFailed(event_id='event_id',
                                                      reason='reas'))

    def test_decision_event_acitivity_timedout(self):
        from flowy.swf import _decision_event, _ActivityTimedout
        event = _decision_event({
            'eventType': 'ActivityTaskTimedOut',
            'activityTaskTimedOutEventAttributes': {
                'scheduledEventId': 'event_id',
            },
        })
        self.assertTypedEquals(event, _ActivityTimedout(event_id='event_id'))

    def test_decision_event_workflow_started(self):
        from flowy.swf import _decision_event, _WorkflowStarted
        event = _decision_event({
            'eventType': 'WorkflowExecutionStarted',
            'workflowExecutionStartedEventAttributes': {'input': 'in'},
        })
        self.assertTypedEquals(event, _WorkflowStarted(input='in'))

    def test_decision_event_decision_completed(self):
        from flowy.swf import _decision_event, _DecisionCompleted
        event = _decision_event({
            'eventType': 'DecisionTaskCompleted',
            'decisionTaskCompletedEventAttributes': {
                'startedEventId': 'event_id',
                'executionContext': 'ctx'
            }
        })
        self.assertTypedEquals(event, _DecisionCompleted(started_by='event_id',
                                                         context='ctx'))

    def test_decision_page(self):
        from flowy.swf import _decision_page, _DecisionPage
        response = {
            'workflowType': {'name': 'wfname', 'version': 'wfversion'},
            'taskToken': 'token',
            'nextPageToken': 'page_token',
            'previousStartedEventId': 'previous_id',
            'events': [sentinel.e1, sentinel.e2, sentinel.e3, sentinel.e4],
        }
        event_maker = Mock()
        event_maker.side_effect = sentinel.r1, None, sentinel.r2, sentinel.r3
        self.assertTypedEquals(
            _decision_page(response, event_maker=event_maker),
            _DecisionPage(
                name='wfname',
                version='wfversion',
                token='token',
                next_page_token='page_token',
                last_event_id='previous_id',
                events=[sentinel.r1, sentinel.r2, sentinel.r3],
            )
        )
        event_maker.assert_has_calls([call(sentinel.e1),
                                      call(sentinel.e2),
                                      call(sentinel.e3),
                                      call(sentinel.e4)])

    def test_poll_decision_page(self):
        from boto.swf.exceptions import SWFResponseError
        from flowy.swf import _repeated_poller
        valid = {'taskToken': 'token', 'other': 'fields'}
        poller = Mock()
        poller.side_effect = [SWFResponseError(0, 0), {}] * 2 + [valid]
        result_klass = Mock()
        _repeated_poller(poller, page_token='token',
                         result_klass=result_klass)
        result_klass.assert_called_once_with(valid)

    def test_poll_decision_page_retries(self):
        from boto.swf.exceptions import SWFResponseError
        from flowy.swf import _repeated_poller
        valid = {'taskToken': 'token', 'other': 'fields'}
        poller = Mock()
        poller.side_effect = [SWFResponseError(0, 0), {}] * 5 + [valid]
        result_klass = Mock()
        result = _repeated_poller(poller, page_token='token',
                                  result_klass=result_klass, retries=4)
        self.assertEquals(result, None)

    def test_poll_decision_collapsed(self):
        from flowy.swf import _poll_decision_collapsed
        from flowy.swf import _DecisionPage, _DecisionCollapsed
        poller = Mock()
        poller.side_effect = [
            _DecisionPage(name='wfname', version='wfversion', token='token',
                          last_event_id='previous_id',
                          next_page_token='page_token1',
                          events=[sentinel.e1, sentinel.e2]),
            _DecisionPage(name='wfname', version='wfversion', token='token',
                          last_event_id='previous_id',
                          next_page_token='page_token2',
                          events=[sentinel.e3, sentinel.e4]),
            _DecisionPage(name='wfname', version='wfversion', token='token',
                          last_event_id='previous_id',
                          next_page_token=None,
                          events=[sentinel.e5]),
        ]
        result = _poll_decision_collapsed(poller)
        self.assertTypedEquals(
            result,
            _DecisionCollapsed(name='wfname', version='wfversion',
                               token='token', last_event_id='previous_id',
                               all_events=ANY)
        )
        self.assertEquals(
            list(result.all_events),
            [sentinel.e1, sentinel.e2, sentinel.e3, sentinel.e4, sentinel.e5]
        )
        poller.assert_has_calls([
            call(),
            call(retries=3, next_page_token='page_token1'),
            call(retries=3, next_page_token='page_token2')
        ])

    def test_decision_response_first_run(self):
        from flowy.swf import _decision_response, _DecisionResponse
        from flowy.swf import _WorkflowStarted, _DecisionCollapsed
        collapsed = _DecisionCollapsed(name='wfname', version='wfversion',
                                       last_event_id=0, token='token',
                                       all_events=[_WorkflowStarted('input')])
        response = _decision_response(collapsed)
        self.assertEquals(
            response,
            _DecisionResponse(name='wfname', version='wfversion',
                              token='token', first_run=True, data='input',
                              new_events=tuple())
        )

    def test_decision_response_nth_run(self):
        from flowy.swf import _decision_response, _DecisionResponse
        from flowy.swf import _DecisionCompleted, _DecisionCollapsed
        all_events = [sentinel.e3, sentinel.e2, sentinel.e1,
                      _DecisionCompleted('last_id', 'ctx')]
        collapsed = _DecisionCollapsed(name='wfname', version='wfversion',
                                       last_event_id='last_id', token='token',
                                       all_events=all_events)
        response = _decision_response(collapsed)
        self.assertEquals(
            response,
            _DecisionResponse(name='wfname', version='wfversion',
                              token='token', first_run=False, data='ctx',
                              new_events=(sentinel.e1,
                                          sentinel.e2,
                                          sentinel.e3))
        )


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
        self.assertTrue(r)
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
        self.assertFalse(r)

    def test_schedule_activities(self):
        m, c = self._get_uut()
        c.queue_activity(call_id='call1', name='name', version=3, input='i')
        c.queue_activity(call_id='call2', name='name', version=3, input='i',
                         heartbeat=10, schedule_to_close=11, task_list='tl',
                         schedule_to_start=12, start_to_close=13)
        r = c.schedule_activities(token='tok', context='ctx')
        self.assertTrue(r)
        m.respond_decision_task_completed.assert_called_with(
            task_token='tok', decisions=[
                {
                    'scheduleActivityTaskDecisionAttributes': {
                        'input': 'i',
                        'activityId': 'call1',
                        'activityType': {
                            'version': '3',
                            'name': 'name'
                        }
                    },
                    'decisionType': 'ScheduleActivityTask'
                },
                {
                    'scheduleActivityTaskDecisionAttributes': {
                        'taskList': {
                            'name': 'tl'
                        },
                        'scheduleToCloseTimeout': '11',
                        'activityType': {
                            'version': '3',
                            'name': 'name'
                        },
                        'heartbeatTimeout': '10',
                        'activityId': 'call2',
                        'scheduleToStartTimeout': '12',
                        'input': 'i',
                        'startToCloseTimeout': '13'
                    },
                    'decisionType': 'ScheduleActivityTask'
                }
            ], execution_context='ctx'
        )

    def test_schedule_activities_error(self):
        m, c = self._get_uut()
        from boto.swf.exceptions import SWFResponseError
        err = SWFResponseError(0, 0)
        m.respond_decision_task_completed.side_effect = err
        c.queue_activity(call_id='call1', name='name', version=3, input='i')
        c.queue_activity(call_id='call2', name='name', version=3, input='i',
                         heartbeat=10, schedule_to_close=11, task_list='tl',
                         schedule_to_start=12, start_to_close=13)
        r = c.schedule_activities(token='tok', context='ctx')
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


class DecisionClientTest(TestCaseNoLogging):
    def _get_uut(self, token='token'):
        from flowy.swf import DecisionClient, JSONDecisionData, SWFClient
        swf = create_autospec(SWFClient, instance=True)
        dd = create_autospec(JSONDecisionData, instance=True)
        return dd, swf, DecisionClient(client=swf, token=token,
                                       decision_data=dd)

    def test_schedule_activities(self):
        dd, swf, dc = self._get_uut(token='tok')
        dd.serialize.return_value = 'ctx'
        swf.schedule_activities.return_value = sentinel.r
        result = dc.schedule_activities(context='newcontext')
        self.assertEquals(result, sentinel.r)
        swf.schedule_activities.assert_called_once_with(token='tok',
                                                        context='ctx')
        dd.serialize.assert_called_once_with('newcontext')

    def test_queue_activity(self):
        dd, swf, dc = self._get_uut(token='tok')
        dc.queue_activity('call_id', 'name', 'version', 'input')
        swf.queue_activity.assert_called_once_with(
            name='name', schedule_to_start=None, start_to_close=None,
            schedule_to_close=None, version='version', context=None,
            task_list=None, heartbeat=None, call_id='call_id', input='input'
        )

    def test_complete(self):
        dd, swf, dc = self._get_uut(token='tok')
        swf.complete_workflow.return_value = sentinel.r
        self.assertEquals(dc.complete('result'), sentinel.r)
        swf.complete_workflow.assert_called_once_with(
            token='tok', result='result'
        )

    def test_fail(self):
        dd, swf, dc = self._get_uut(token='tok')
        swf.fail_workflow.return_value = sentinel.r
        self.assertEquals(dc.fail('reason'), sentinel.r)
        swf.fail_workflow.assert_called_once_with(
            token='tok', reason='reason'
        )


class JSONDecisionContext(unittest.TestCase):
    def _get_uut(self, context=None):
        from flowy.swf import JSONDecisionContext
        return JSONDecisionContext(context)

    def test_empty(self):
        dc = self._get_uut()
        self.assertEquals(dc.global_context(), None)
        self.assertEquals(dc.activity_context('a1'), None)
        self.assertEquals(dc.global_context(default=sentinel.gc), sentinel.gc)
        a1 = sentinel.a1
        self.assertEquals(dc.activity_context('a1', default=a1), a1)
        dc = self._get_uut(context=dc.serialize())
        self.assertEquals(dc.global_context(), None)
        self.assertEquals(dc.activity_context('a1'), None)
        self.assertEquals(dc.global_context(default=sentinel.gc), sentinel.gc)
        a1 = sentinel.a1
        self.assertEquals(dc.activity_context('a1', default=a1), a1)

    def test_global_context(self):
        dc = self._get_uut(self._get_uut().serialize('global_context'))
        self.assertEquals(dc.global_context(), 'global_context')

    def test_activity_context(self):
        dc = self._get_uut()
        dc.set_activity_context('a1', 'a1context')
        self.assertEquals(dc.activity_context('a1'), 'a1context')
        dc = self._get_uut(dc.serialize())
        self.assertEquals(dc.activity_context('a1'), 'a1context')

    def test_mapping(self):
        dc = self._get_uut()
        dc.map_event_to_call('e1', 'c1')
        self.assertEquals(dc.event_to_call('e1'), 'c1')
        dc = self._get_uut(dc.serialize())
        self.assertEquals(dc.event_to_call('e1'), 'c1')


class JSONDecisoinData(unittest.TestCase):
    def _get_uut(self, data, first_run=True):
        from flowy.swf import JSONDecisionData
        if first_run:
            return JSONDecisionData.for_first_run(data)
        return JSONDecisionData(data)

    def test_empty_context(self):
        input = 'input'
        dd = self._get_uut(input)
        self.assertEquals(dd.input, 'input')
        self.assertEquals(dd.context, None)
        dd = self._get_uut(dd.serialize(), first_run=False)
        self.assertEquals(dd.input, 'input')
        self.assertEquals(dd.context, None)

    def test_context(self):
        input = 'input'
        dd = self._get_uut(input)
        dd = self._get_uut(dd.serialize('new_context'), first_run=False)
        self.assertEquals(dd.input, 'input')
        self.assertEquals(dd.context, 'new_context')
        dd = self._get_uut(dd.serialize('new_context2'), first_run=False)
        self.assertEquals(dd.input, 'input')
        self.assertEquals(dd.context, 'new_context2')


class DecisionTest(unittest.TestCase):
    def _get_uut(self, events=[]):
        from flowy.swf import Decision
        from flowy.swf import JSONDecisionContext, DecisionClient
        cli = create_autospec(DecisionClient, instance=True)
        ctx = create_autospec(JSONDecisionContext, instance=True)
        return cli, ctx, Decision(cli, ctx, events)

    def test_event_to_call(self):
        from flowy.swf import _ActivityScheduled
        cli, ctx, d = self._get_uut([
            _ActivityScheduled('e1', 'c1'),
            _ActivityScheduled('e2', 'c2'),
            _ActivityScheduled('e3', 'c3'),
        ])
        ctx.map_event_to_call.assert_has_calls([
            call('e1', 'c1'),
            call('e2', 'c2'),
            call('e3', 'c3'),
        ])

    def test_events_dispatch(self):
        from flowy.swf import _ActivityScheduled, _ActivityTimedout
        from flowy.swf import _ActivityCompleted, _ActivityFailed
        cli, ctx, d = self._get_uut([
            _ActivityScheduled('xx', 'yy'),
            _ActivityTimedout('e1'),
            _ActivityCompleted('e2', 'result'),
            _ActivityFailed('e3', 'reason'),
        ])
        ctx.event_to_call.side_effect = ['c10', 'c11', 'c12', 'c13']
        m = Mock()
        d.dispatch_new_events(m)
        ctx.event_to_call.assert_has_calls([
            call('e1'), call('e2'), call('e3')
        ])
        m.activity_scheduled.assert_called_once_with('c10')
        m.activity_timedout.assert_called_once_with('c11')
        m.activity_completed.assert_called_once_with('c12', 'result')
        m.activity_failed.assert_called_once_with('c13', 'reason')

    def test_events_dispatch_to_empty(self):
        from flowy.swf import _ActivityScheduled, _ActivityTimedout
        from flowy.swf import _ActivityCompleted, _ActivityFailed
        cli, ctx, d = self._get_uut([
            _ActivityScheduled('xx', 'yy'),
            _ActivityTimedout('e1'),
            _ActivityCompleted('e2', 'result'),
            _ActivityFailed('e3', 'reason'),
        ])
        try:
            d.dispatch_new_events, object()
        except AttributeError:
            self.fail('Dispatch to empty object not working.')

    def test_queue_activity(self):
        cli, ctx, d = self._get_uut()
        d.queue_activity('call_id', 'name', 'version', 'input', context='ctx')
        ctx.set_activity_context.assert_called_once_with('call_id', 'ctx')
        cli.queue_activity.assert_called_once_with(
            call_id='call_id', name='name', version='version', input='input',
            heartbeat=None, schedule_to_close=None, schedule_to_start=None,
            start_to_close=None, task_list=None
        )

    def test_queue_activities(self):
        cli, ctx, d = self._get_uut()
        ctx.serialize.return_value = 'ctx'
        d.schedule_activities('context')
        ctx.serialize.assert_called_once_with('context')
        cli.schedule_activities.assert_called_with('ctx')


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


class ClientTest(unittest.TestCase):
    def _get_uut(self):
        from flowy.swf import Client, SWFClient
        client = create_autospec(SWFClient, instance=True)
        return client, Client(client)

    def test_registration(self):
        c, wc = self._get_uut()
        wc.register_workflow(Mock(), 'name', 'version', 'task_list', 120, 30)
        c.register_workflow.assert_called_once_with(
            name='name', version='version', task_list='task_list',
            execution_start_to_close=120, task_start_to_close=30,
            child_policy='TERMINATE', descr=None
        )

    def test_empty(self):
        from flowy.swf import _DecisionResponse
        c, wc = self._get_uut()
        c.poll_decision.return_value = _DecisionResponse(
            name='name',
            version='v1',
            new_events=[],
            token='tok',
            data='data',
            first_run=False
        )
        self.assertEquals(wc.dispatch_next_decision('task_list'), None)
        c.poll_decision.assert_called_once_with('task_list')
