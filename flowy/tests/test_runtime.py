import unittest

from flowy.tests.test_spec import swf_error
from mock import sentinel as s
from mock import Mock, call, ANY


class TestHeartbeat(unittest.TestCase):

    def _get_uut(self, client=None, token=s.token):
        from flowy.runtime import Heartbeat
        if client is None:
            client = Mock()
        return Heartbeat(client, token), client

    def test_call(self):
        hb, client = self._get_uut()
        result = hb()
        client.record_activity_task_heartbeat.assert_called_once_with(
            task_token=s.token
        )
        self.assertTrue(result)

    def test_error(self):
        hb, client = self._get_uut()
        client.record_activity_task_heartbeat.side_effect = swf_error()
        result = hb()
        self.assertFalse(result)


class TestBoundProxy(unittest.TestCase):
    def _get_uut(self, runtime=s.dr, decision_task=None):
        from flowy.runtime import BoundProxyRuntime
        if decision_task is None:
            decision_task = Mock()
        return BoundProxyRuntime(runtime, decision_task), decision_task

    def test_proxy(self):
        bpr, decision_task = self._get_uut()
        bpr.xyz(s.x, s.y)
        decision_task.xyz.assert_called_once_with(s.dr, s.x, s.y)


class TestContextOptions(unittest.TestCase):
    def _get_uut(self, decision_runtime=None):
        from flowy.runtime import ContextOptionsRuntime
        if decision_runtime is None:
            decision_runtime = Mock()
        return ContextOptionsRuntime(decision_runtime), decision_runtime

    def test_remote_activity_defaults(self):
        co, d_runtime = self._get_uut()
        co.remote_activity(s.deserializer)
        d_runtime.remote_activity.assert_called_once_with(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            task_list=None,
            retry=3,
            delay=0,
            error_handling=False,
            result_deserializer=s.deserializer
        )

    def test_remote_workflow_defaults(self):
        co, d_runtime = self._get_uut()
        co.remote_subworkflow(s.deserializer)
        d_runtime.remote_subworkflow.assert_called_once_with(
            heartbeat=None,
            workflow_duration=None,
            decision_duration=None,
            task_list=None,
            retry=3,
            delay=0,
            error_handling=False,
            result_deserializer=s.deserializer
        )

    def test_remote_activity_other_values(self):
        co, d_runtime = self._get_uut()
        co.remote_activity(
            s.deserializer,
            heartbeat=5,
            schedule_to_close=10,
            schedule_to_start=15,
            start_to_close=20,
            task_list='list',
            retry=5,
            delay=10
        )
        d_runtime.remote_activity.assert_called_once_with(
            heartbeat=5,
            schedule_to_close=10,
            schedule_to_start=15,
            start_to_close=20,
            task_list='list',
            retry=5,
            delay=10,
            error_handling=False,
            result_deserializer=s.deserializer
        )

    def test_remote_activity_options_stack(self):
        co, d_runtime = self._get_uut()
        with co.options(retry=10, heartbeat=50):
            co.remote_activity(s.deserializer, heartbeat=5, retry=5)
            with co.options(retry=11):
                co.remote_activity(s.deserializer, heartbeat=5, retry=5)
            co.remote_activity(s.deserializer, heartbeat=5, retry=5)
        outer_call = call(
            heartbeat=50,
            schedule_to_close=ANY,
            schedule_to_start=ANY,
            start_to_close=ANY,
            task_list=ANY,
            retry=10,
            delay=ANY,
            error_handling=ANY,
            result_deserializer=ANY
        )
        d_runtime.remote_activity.has_calls([
            outer_call,
            call(
                heartbeat=50,
                schedule_to_close=ANY,
                schedule_to_start=ANY,
                start_to_close=ANY,
                task_list=ANY,
                retry=11,
                delay=ANY,
                error_handling=ANY,
                result_deserializer=ANY
            ),
            outer_call
        ])
