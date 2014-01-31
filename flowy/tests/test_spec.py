import unittest

from mock import sentinel as s
from mock import Mock


class TestTaskSpec(unittest.TestCase):
    def _get_uut(self, task_id=None):
        from flowy.spec import TaskSpec
        return TaskSpec(task_id=task_id, task_factory=s.task_factory)

    def test_registration(self):
        worker = Mock()
        uut = self._get_uut(task_id=s.task_id)
        uut.register(worker)
        worker.register.assert_called_once_with(
            task_id=s.task_id,
            task_factory=s.task_factory
        )
