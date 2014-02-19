import unittest

from mock import sentinel as s
from mock import Mock


class TestTask(unittest.TestCase):
    def _get_uut(self, input='[[], {}]', scheduler=None):
        from flowy.task import Task
        if scheduler is None:
            scheduler = Mock()
        return Task(input, scheduler), scheduler

    def test_successful_run(self):
        task, scheduler = self._get_uut(
            input='[[1, "a"], {"x": 2, "y": "y"}]',
        )
        task.run = Mock()
        task.run.return_value = [1, 2, 'a']
        task()
        task.run.assert_called_once_with(1, 'a', x=2, y='y')
        scheduler.complete.assert_called_once_with('[1, 2, "a"]')

    def test_suspend_task(self):
        task, scheduler = self._get_uut()
        task.run = Mock()
        from flowy.task import SuspendTask
        task.run.side_effect = SuspendTask()
        task()
        task.run.assert_called_once_with()
        scheduler.suspend.assert_called_once_with()

    def test_fail_task(self):
        task, scheduler = self._get_uut()
        task.run = Mock()
        task.run.side_effect = RuntimeError('err')
        task()
        task.run.assert_called_once_with()
        scheduler.fail.assert_called_once_with('err')


class TestHeartbeat(unittest.TestCase):
    def test_heartbeat(self):
        from flowy.task import Activity
        scheduler = Mock()
        a = Activity(input='[[], {}]', scheduler=scheduler)
        a.heartbeat()
        scheduler.heartbeat.assert_called_once_with()


class TestWorkflow(unittest.TestCase):
    def _get_uut(self):
        from flowy.task import Workflow
        scheduler = Mock()
        return Workflow(input='[[], {}]', scheduler=scheduler), scheduler

    def test_restart(self):
        uut, scheduler = self._get_uut()
        uut.restart(1, 2, a=1, b=2)
        scheduler.restart.assert_called_once_with('[[1, 2], {"a": 1, "b": 2}]')

    def test_options(self):
        uut, scheduler = self._get_uut()
        uut.options(x=1, y=2, z='abc')
        scheduler.options.assert_called_once_with(x=1, y=2, z='abc')

    def test_run_return_error(self):
        from flowy.result import Error
        uut, scheduler = self._get_uut()
        uut.run = Mock()
        uut.run.return_value = Error('reason')
        uut()
        uut.run.assert_called_once_with()
        scheduler.fail.assert_called_once_with('reason')

    def test_run_return_placeholder(self):
        from flowy.result import Placeholder
        uut, scheduler = self._get_uut()
        uut.run = Mock()
        uut.run.return_value = Placeholder()
        uut()
        uut.run.assert_called_once_with()
        scheduler.suspend.assert_called_once_with()

    def test_run_return_result(self):
        from flowy.result import Result
        uut, scheduler = self._get_uut()
        uut.run = Mock()
        uut.run.return_value = Result(12)
        uut()
        uut.run.assert_called_once_with()
        scheduler.complete.assert_called_once_with('12')

    def test_run_return_value(self):
        uut, scheduler = self._get_uut()
        uut.run = Mock()
        uut.run.return_value = 12
        uut()
        uut.run.assert_called_once_with()
        scheduler.complete.assert_called_once_with('12')


class TestTaskProxy(unittest.TestCase):
    def _get_uut(self, args=[], kwargs={}):
        from flowy.task import Task, TaskProxy
        tp = TaskProxy()
        input = tp._serialize_arguments(*args, **kwargs)
        t = Task(input=input, scheduler=Mock())
        return tp, t

    def test_results(self):
        tp, t = self._get_uut()
        r = tp._deserialize_result(t._serialize_result([1, 'a']))
        self.assertEquals(r, [1, 'a'])


class TestActivityProxy(unittest.TestCase):
    def _get_uut(self):
        from flowy.task import ActivityProxy
        return ActivityProxy(
            task_id=s.task_id,
            heartbeat=-10,
            schedule_to_close='100',
            schedule_to_start=20,
            start_to_close=20.2,
            task_list=s.task_list,
            retry=-3,
            delay='10',
            error_handling=s.error_handling
        )

    def test_binding(self):

        class X(object):
            _scheduler = Mock()
            a = self._get_uut()

        x = X()
        x.a(1, 2, a=1, b=2)

        X._scheduler.remote_activity.assert_called_once_with(
            args=(1, 2),
            kwargs=dict(a=1, b=2),
            args_serializer=X.a._serialize_arguments,
            result_deserializer=X.a._deserialize_result,
            task_id=s.task_id,
            heartbeat=None,
            schedule_to_close=100,
            schedule_to_start=20,
            start_to_close=20,
            task_list='sentinel.task_list',
            retry=0,
            delay=10,
            error_handling=True
        )

    def test_no_scheduler(self):
        class X(object):
            a = self._get_uut()

        x = X()
        self.assertRaises(AttributeError, lambda: x.a())


class TestWorkflowProxy(unittest.TestCase):
    def _get_uut(self):
        from flowy.task import WorkflowProxy
        return WorkflowProxy(
            task_id=s.task_id,
            workflow_duration=-10,
            decision_duration='100',
            task_list=s.task_list,
            retry=-3,
            delay='10',
            error_handling=s.error_handling
        )

    def test_binding(self):

        class X(object):
            _scheduler = Mock()
            a = self._get_uut()

        x = X()
        x.a(1, 2, a=1, b=2)

        X._scheduler.remote_subworkflow.assert_called_once_with(
            args=(1, 2),
            kwargs=dict(a=1, b=2),
            args_serializer=X.a._serialize_arguments,
            result_deserializer=X.a._deserialize_result,
            task_id=s.task_id,
            workflow_duration=None,
            decision_duration=100,
            task_list='sentinel.task_list',
            retry=0,
            delay=10,
            error_handling=True
        )
