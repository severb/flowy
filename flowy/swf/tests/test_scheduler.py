import unittest

from mock import Mock
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
            token=s.token
        )

    def test_heartbeat_error(self):
        a_s, client = self._get_uut()
        client.record_activity_task_heartbeat.side_effect = swf_error()
        self.assertFalse(a_s.heartbeat())
