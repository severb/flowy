import unittest

from mock import Mock
from mock import sentinel as s

from flowy.result import Placeholder


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
            token=s.token
        )

    def test_heartbeat_error(self):
        a_s, client = self._get_uut()
        client.record_activity_task_heartbeat.side_effect = swf_error()
        self.assertFalse(a_s.heartbeat())

    def test_complete(self):
        a_s, client = self._get_uut()
        self.assertTrue(a_s.complete('result'))
        client.respond_activity_task_completed.assert_called_once_with(
            result='result', token=s.token
        )

    def test_complete_error(self):
        a_s, client = self._get_uut()
        client.respond_activity_task_completed.side_effect = swf_error()
        self.assertFalse(a_s.complete('result'))

    def test_fail(self):
        a_s, client = self._get_uut()
        self.assertTrue(a_s.fail('reason'))
        client.respond_activity_task_failed.assert_called_once_with(
            reason='reason', token=s.token
        )

    def test_fail_error(self):
        a_s, client = self._get_uut()
        client.respond_activity_task_failed.side_effect = swf_error()
        self.assertFalse(a_s.fail('result'))

class DecisionSchedulerTest(unittest.TestCase):
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
        d_s._decisions = Mock()
        d_s._decisions._data = False
        return d_s, client, d_s._decisions

    def test_complete(self):
        uut, client, decisions = self._get_uut()
        self.assertTrue(uut.complete('result'))
        decisions.complete_workflow_execution.assert_called_once_with(
            result='result'
        )

    def test_complete_with_running_tasks(self):
        uut, client, decisions = self._get_uut(running=[0])
        self.assertTrue(uut.complete('result'))
        self.assertEqual(decisions.complete_workflow_execution.call_count, 0)

    def test_suspend_fail(self):
        uut, client, decisions = self._get_uut()
        client.respond_decision_task_completed.side_effect = swf_error()
        self.assertFalse(uut.suspend())

    def test_fail(self):
        uut, client, decisions = self._get_uut()
        self.assertTrue(uut.fail('result'))
        self.assertNotEqual(decisions, uut._decisions)

    def test_reserving_call_ids(self):
        uut, client, decisions = self._get_uut()
        uut._reserve_call_ids(5, 3, 10)
        self.assertEqual(uut._call_id, 17)
        uut._reserve_call_ids(1, 0, 0)
        self.assertEqual(uut._call_id, 2)

    def test_no_timer_result(self):
        uut, client, decisions = self._get_uut()
        self.assertIsNone(uut._timer_result(0))

    def test_queue_timer(self):
        uut, client, decisions = self._get_uut()
        res = uut._timer_result(1)
        self.assertIsInstance(res, Placeholder)
        self.assertTrue(False)  # queuing is not yet implemented

    def test_timer_in_progress(self):
        uut, client, decisions = self._get_uut(running=[0])
        res = uut._timer_result(1)
        self.assertIsInstance(res, Placeholder)

    def test_timer_in_progress(self):
        uut, client, decisions = self._get_uut(results={0: 'result'})
        res = uut._timer_result(1)
        self.assertEqual(1, uut._call_id)

    def test_remote_activity_with_delay(self):
        uut, client, decisions = self._get_uut(running=[0])
        res = uut.remote_activity(s.id, s.input, s.des, s.hearbeat,
                                  s.sched_to_close, s.sched_to_start,
                                  s.st_to_close, s.tl, 0, 1, False)
        self.assertIsInstance(res, Placeholder)

    def test_remote_activity_running(self):
        uut, client, decisions = self._get_uut(running=[0])
        res = uut.remote_activity(s.id, s.input, s.des, s.hearbeat,
                                  s.sched_to_close, s.sched_to_start,
                                  s.st_to_close, s.tl, 0, 0, False)
        self.assertIsInstance(res, Placeholder)

    def test_remote_activity_not_scheduled(self):
        uut, client, decisions = self._get_uut()
        res = uut.remote_activity((s.name, s.v), s.input, s.des, s.hearbeat,
                                  s.sched_to_close, s.sched_to_start,
                                  s.st_to_close, s.tl, 0, 0, False)
        self.assertIsInstance(res, Placeholder)
        self.assertEquals(1, uut._call_id)

    def test_remote_workflow_with_delay(self):
        uut, client, decisions = self._get_uut(running=[0])
        res = uut.remote_subworkflow(s.id, s.input, s.des, s.wf_dur, s.dec_dur,
                                     s.tl, 0, 1, False)
        self.assertIsInstance(res, Placeholder)

    def test_remote_workflow_running(self):
        uut, client, decisions = self._get_uut(running=[0])
        res = uut.remote_subworkflow(s.id, s.input, s.des, s.wf_dur, s.dec_dur,
                                     s.tl, 0, 0, False)
        self.assertIsInstance(res, Placeholder)

    def test_remote_workflow_not_scheduled(self):
        uut, client, decisions = self._get_uut()
        res = uut.remote_subworkflow((s.name, s.v), s.input, s.des, s.wf_dur,
                                     s.dec_dur, s.tl, 0, 0, False)
        self.assertIsInstance(res, Placeholder)
        self.assertEquals(1, uut._call_id)

