import unittest

from mock import sentinel as s
from mock import Mock, call, patch


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

        uut.scan_workflows(package=flowy.tests.specs)

        collector.collect.assert_has_calls([
            call(
                task_id=s.task_id,
                task_factory=s.task_factory3,
                task_list='sentinel.task_list',
                workflow_duration=3600,
                decision_duration=60),
            call(
                task_id=s.task_id,
                task_factory=s.task_factory4,
                task_list='sentinel.task_list',
                workflow_duration=120,
                decision_duration=5)
        ])

    def test_activity_errors(self):
        from flowy.scanner import activity

        self.assertRaises(ValueError, activity, s.id, s.tl, heartbeat=-1)
        self.assertRaises(ValueError, activity, s.id, s.tl,
                          schedule_to_close=-1)
        self.assertRaises(ValueError, activity, s.id, s.tl,
                          schedule_to_start=-1)
        self.assertRaises(ValueError, activity, s.id, s.tl, start_to_close=-1)
        self.assertRaises(ValueError, activity, s.id, s.tl, heartbeat='ab')

    def test_workflow_errors(self):
        from flowy.scanner import workflow

        self.assertRaises(ValueError, workflow, s.id, s.tl,
                          decision_duration=-1)
        self.assertRaises(ValueError, workflow, s.id, s.tl,
                          decision_duration='ab')
        self.assertRaises(ValueError, workflow, s.id, s.tl,
                          workflow_duration=-1)

    @patch('venusian.Scanner')
    def test_default_package(self, mock_scanner):
        import flowy.tests
        uut, _ = self._get_uut()
        uut.scan_activities()
        mock_scanner().scan.assert_called_once_with(
            flowy.tests, categories=('activity',), ignore=None
        )
