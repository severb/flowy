import unittest

from mock import sentinel as s
from mock import Mock


class TestTask(unittest.TestCase):

    def _get_uut(self, input='[[], {}]', runtime=None):
        from flowy.task import Task
        if runtime is None:
            runtime = Mock()
        return Task(input, runtime), runtime

    def test_successful_run(self):
        task, runtime = self._get_uut(
            input='[[1, "a"], {"x": 2, "y": "y"}]',
        )
        task.run = Mock()
        task.run.return_value = [1, 2, 'a']
        task()
        task.run.assert_called_once_with(1, 'a', x=2, y='y')

    def test_suspend_task(self):
        task, runtime = self._get_uut()
        task.run = Mock()
        from flowy.task import SuspendTask
        task.run.side_effect = SuspendTask()
        task()
        task.run.assert_called_once_with()

    def test_fail_task(self):
        task, runtime = self._get_uut()
        task.run = Mock()
        task.run.side_effect = RuntimeError('err')
        task()
        task.run.assert_called_once_with()


class TestTransport(unittest.TestCase):
    def test_arguments(self):
        from flowy.task import Task, TaskProxy
        tp = TaskProxy(None, None)
        serialized_input = tp._serialize_arguments(1, 'a', x=2)
        t = Task(input=serialized_input, runtime=Mock())
        a, kw = t._deserialize_arguments()
        self.assertEquals(a, [1, 'a'])
        self.assertEquals(kw, {'x': 2})

    def test_results(self):
        from flowy.task import Task, TaskProxy
        tp = TaskProxy(None, None)
        t = Task(None, None)
        r = tp._deserialize_result(t._serialize_result([1, 'a']))
        self.assertEquals(r, [1, 'a'])
