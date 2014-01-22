import unittest

from mock import sentinel as s
from mock import Mock


class TestTask(unittest.TestCase):

    def _get_uut(self, input='[[], {}]', result=None, task_runtime=s.runtime):
        from flowy.task import Task
        if result is None:
            result = Mock()
        return Task(input, result, task_runtime), result

    def test_successful_run(self):
        task, result = self._get_uut(
            input='[[1, "a"], {"x": 2, "y": "y"}]',
            task_runtime=s.runtime
        )
        task.run = Mock()
        task.run.return_value = [1, 2, 'a']
        task()
        task.run.assert_called_once_with(s.runtime, 1, 'a', x=2, y='y')
        result.complete.assert_called_once_with('[1, 2, "a"]')

    def test_suspend_task(self):
        task, result = self._get_uut()
        task.run = Mock()
        from flowy.task import SuspendTask
        task.run.side_effect = SuspendTask()
        task()
        task.run.assert_called_once_with(s.runtime)
        self.assertTrue(result.suspend.called)

    def test_fail_task(self):
        task, result = self._get_uut()
        task.run = Mock()
        task.run.side_effect = RuntimeError('err')
        task()
        task.run.assert_called_once_with(s.runtime)
        result.fail.assert_called_once_with('err')


class TestTransport(unittest.TestCase):
    def test_arguments(self):
        from flowy.task import Task, TaskProxy
        tp = TaskProxy()
        serialized_input = tp.serialize_arguments(1, 'a', x=2)
        t = Task(input=serialized_input, result=None, task_runtime=Mock())
        a, kw = t.deserialize_arguments()
        self.assertEquals(a, [1, 'a'])
        self.assertEquals(kw, {'x': 2})

    def test_results(self):
        from flowy.task import Task, TaskProxy
        tp = TaskProxy()
        t = Task(None, None, Mock())
        r = tp.deserialize_result(t.serialize_result([1, 'a']))
        self.assertEquals(r, [1, 'a'])
