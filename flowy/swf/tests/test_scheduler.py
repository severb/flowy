import unittest

from mock import Mock, ANY
from mock import sentinel as s


def swf_error():
    from boto.swf.exceptions import SWFResponseError
    return SWFResponseError(0, 0)


class ActivitySchedulerTest(unittest.TestCase):
    def _get_uut(self):
        from flowy.swf.scheduler import ActivityScheduler
        client = Mock()
        return ActivityScheduler(client, s.token), client

    def test_heartbeat(self):
        a_s, client = self._get_uut()
        self.assertTrue(a_s.heartbeat())
        client.record_activity_task_heartbeat.assert_called_once_with(
            task_token=s.token
        )

    def test_heartbeat_error(self):
        a_s, client = self._get_uut()
        client.record_activity_task_heartbeat.side_effect = swf_error()
        self.assertFalse(a_s.heartbeat())

    def test_complete(self):
        a_s, client = self._get_uut()
        self.assertTrue(a_s.complete('result'))
        client.respond_activity_task_completed.assert_called_once_with(
            result='result', task_token=s.token
        )

    def test_complete_error(self):
        a_s, client = self._get_uut()
        client.respond_activity_task_completed.side_effect = swf_error()
        self.assertFalse(a_s.complete('result'))

    def test_fail(self):
        a_s, client = self._get_uut()
        self.assertTrue(a_s.fail('reason'))
        client.respond_activity_task_failed.assert_called_once_with(
            reason='reason', task_token=s.token
        )

    def test_fail_error(self):
        a_s, client = self._get_uut()
        client.respond_activity_task_failed.side_effect = swf_error()
        self.assertFalse(a_s.fail('result'))


class DecisionSchedulerTest(unittest.TestCase):

    subworkflow_decision = {
        'startChildWorkflowExecutionDecisionAttributes': {
            'workflowId': ANY,
            'taskList': {'name': 'sentinel.task_list'},
            'taskStartToCloseTimeout': '20',
            'executionStartToCloseTimeout': '10',
            'input': 'input',
            'workflowType': {
                'version': 'version',
                'name': 'name'
            }
        },
        'decisionType': 'StartChildWorkflowExecution'
    }

    def activity_decision(self, id):
        return {
            'scheduleActivityTaskDecisionAttributes': {
                'taskList': {'name': 'sentinel.task_list'},
                'scheduleToCloseTimeout': '20',
                'activityType': {'version': 'version', 'name': 'name'},
                'heartbeatTimeout': '10',
                'activityId': str(id),
                'scheduleToStartTimeout': '30',
                'input': 'input',
                'startToCloseTimeout': '40'
            },
            'decisionType': 'ScheduleActivityTask'
        }

    def _get_uut(self, running=None, timedout=None, results=None, errors=None):
        from flowy.swf.scheduler import DecisionScheduler
        client = Mock()
        if running is None:
            running = []
        if timedout is None:
            timedout = []
        if results is None:
            results = {}
        if errors is None:
            errors = {}
        d_s = DecisionScheduler(client, s.token, running, timedout, results,
                                errors)
        return d_s, client

    def test_empty_suspend(self):
        uut, client = self._get_uut()
        self.assertTrue(uut.suspend())
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[]
        )

    def test_suspend_error(self):
        uut, client = self._get_uut()
        client.respond_decision_task_completed.side_effect = swf_error()
        self.assertFalse(uut.suspend())

    def test_schedule_activity(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut()
        self.assertIsInstance(uut.remote_activity(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)
        uut.suspend()
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[self.activity_decision(0)]
        )

    def test_schedule_subworkflow(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut()
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)
        uut.suspend()
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[self.subworkflow_decision]
        )

    def test_activity_result_found(self):
        from flowy.swf import SWFTaskId
        uut, client = self._get_uut(results={0: 'result'})
        result_deserializer = Mock()
        result = uut.remote_activity(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=result_deserializer,
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ).result()
        result_deserializer.assert_called_once_with('result')
        self.assertEquals(result, result_deserializer())

    def test_subworkflow_result_found(self):
        from flowy.swf import SWFTaskId
        uut, client = self._get_uut(results={0: 'result'})
        result_deserializer = Mock()
        result = uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ).result()
        result_deserializer.assert_called_once_with('result')
        self.assertEquals(result, result_deserializer())

    def test_delay(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut()
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=1,
            error_handling=False
        ), Placeholder)
        uut.suspend()
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[{
                'startTimerDecisionAttributes': {
                    'timerId': '0',
                    'startToFireTimeout': '1'
                },
                'decisionType': 'StartTimer'
            }]
        )

    def test_running_timer(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut(running=set([0]))
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=1,
            error_handling=False
        ), Placeholder)

    def test_finished_timer(self):
        from flowy.swf import SWFTaskId
        uut, client = self._get_uut(results={0: None, 1: 'result'})
        result_deserializer = Mock()
        result = uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=1,
            error_handling=False
        ).result()
        result_deserializer.assert_called_once_with('result')
        self.assertEquals(result, result_deserializer())

    def test_placeholder_for_running(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut(running=set([0]))
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)

    def test_skip_timeouts(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut(timedout=set([0, 1]), running=set([2]))
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)

    def test_error_with_error_handling(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Error
        uut, client = self._get_uut(errors={0: 'err'})
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=True
        ), Error)

    def test_error_without_error_handling(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut(errors={0: 'err'})
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[{
                'failWorkflowExecutionDecisionAttributes': {
                    'reason': 'err'
                },
                'decisionType': 'FailWorkflowExecution'
            }]
        )

    def test_timedout_with_error_handling(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Timeout
        uut, client = self._get_uut(timedout=set([0, 1, 2, 3]))
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=True
        ), Timeout)

    def test_timedout_without_error_handling(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut(timedout=set([0, 1, 2, 3]))
        self.assertIsInstance(uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)
        uut.suspend()
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[{
                'failWorkflowExecutionDecisionAttributes': {
                    'reason': ANY
                },
                'decisionType': 'FailWorkflowExecution'
            }]
        )

    def test_failing_always_overrides(self):
        from flowy.swf import SWFTaskId
        uut, client = self._get_uut()
        uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        )
        uut.fail(reason='reason')
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[{
                'failWorkflowExecutionDecisionAttributes': {
                    'reason': 'reason'
                },
                'decisionType': 'FailWorkflowExecution'
            }]
        )

    def test_consecutive_scheduling(self):
        from flowy.swf import SWFTaskId
        uut, client = self._get_uut()
        uut.remote_subworkflow(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            workflow_duration=10,
            decision_duration=20,
            task_list=s.task_list,
            retry=10,
            delay=1,
            error_handling=False
        )
        uut.remote_activity(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        )
        uut.remote_activity(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        )
        uut.suspend()
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[{
                'startTimerDecisionAttributes': {
                    'timerId': '0',
                    'startToFireTimeout': '1'
                },
                'decisionType': 'StartTimer'
            }, self.activity_decision(12), self.activity_decision(16)]
        )

    def test_complete_after_scheduling_is_ingnored(self):
        from flowy.swf import SWFTaskId
        from flowy.result import Placeholder
        uut, client = self._get_uut()
        self.assertIsInstance(uut.remote_activity(
            task_id=SWFTaskId(name='name', version='version'),
            input='input',
            result_deserializer=s.result_deserializer,
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            task_list=s.task_list,
            retry=3,
            delay=0,
            error_handling=False
        ), Placeholder)
        uut.complete(result='result')
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[self.activity_decision(0)]
        )

    def test_complete(self):
        uut, client = self._get_uut()
        uut.complete(result='result')
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[{
                'completeWorkflowExecutionDecisionAttributes': {
                    'result': 'result'
                },
                'decisionType': 'CompleteWorkflowExecution'
            }]
        )

    def complete_while_running_is_ignored(self):
        uut, client = self._get_uut(running=set([0]))
        uut.complete(result='result')
        client.respond_decision_task_completed.assert_called_once_with(
            task_token=s.token, decisions=[]
        )

    def test_rate_limit(self):
        from flowy.swf import SWFTaskId
        uut, client = self._get_uut(running=set(range(20)))
        for x in range(200):
            uut.remote_activity(
                task_id=SWFTaskId(name='name', version='version'),
                input='input',
                result_deserializer=s.result_deserializer,
                heartbeat=10,
                schedule_to_close=20,
                schedule_to_start=30,
                start_to_close=40,
                task_list=s.task_list,
                retry=3,
                delay=10,
                error_handling=False
            )
            uut.remote_activity(
                task_id=SWFTaskId(name='name', version='version'),
                input='input',
                result_deserializer=s.result_deserializer,
                heartbeat=10,
                schedule_to_close=20,
                schedule_to_start=30,
                start_to_close=40,
                task_list=s.task_list,
                retry=3,
                delay=0,
                error_handling=False
            )
            uut.remote_subworkflow(
                task_id=SWFTaskId(name='name', version='version'),
                input='input',
                result_deserializer=s.result_deserializer,
                workflow_duration=10,
                decision_duration=20,
                task_list=s.task_list,
                retry=3,
                delay=0,
                error_handling=False
            )
        uut.suspend()
        self.assertEquals(44, len(
            client.respond_decision_task_completed.call_args[1]['decisions']
        ))
