import unittest

from mock import sentinel as s
from mock import Mock, call


class TestRegistrationDecorators(unittest.TestCase):

    def _get_uut(self):
        from flowy.scanner import Scanner
        collector = Mock()
        return Scanner(collector=collector), collector

    def test_activity_detection(self):
        uut, collector = self._get_uut()

        import flowy.tests.specs

        uut.scan_activities(package=flowy.tests.specs)

        collector.collect.assert_has_calls([
            call(
                task_id=s.task_id,
                task_factory=s.task_factory1,
                task_list='sentinel.task_list',
                heartbeat=60,
                schedule_to_close=420,
                schedule_to_start=120,
                start_to_close=300),
            call(
                task_id=s.task_id,
                task_factory=s.task_factory2,
                task_list='sentinel.task_list',
                heartbeat=6,
                schedule_to_close=42,
                schedule_to_start=12,
                start_to_close=30)
        ])
