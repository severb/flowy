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


class TestTaskProxy(unittest.TestCase):
    def _get_uut(self, runtime=s.runtime,  args=[], kwargs={}):
        from flowy.task import Task, TaskProxy
        if runtime == s.runtime:
            runtime = Mock()
        tp = TaskProxy()
        input = tp._serialize_arguments(*args, **kwargs)
        t = Task(input=input, runtime=runtime)
        return tp, t

    def test_arguments(self):
        tp, t = self._get_uut(args=[1, 'a'], kwargs={'x': 2})
        a, kw = t._deserialize_arguments()
        self.assertEquals(a, [1, 'a'])
        self.assertEquals(kw, {'x': 2})

    def test_results(self):
        tp, t = self._get_uut()
        r = tp._deserialize_result(t._serialize_result([1, 'a']))
        self.assertEquals(r, [1, 'a'])

    def test_runtime(self):
        from flowy.task import TaskProxy, Task
        class A(Task):
            tp = TaskProxy()
        x = A.tp
        print(x)
        self.assertTrue(False)
