import unittest

from mock import sentinel as s
from mock import call, Mock


class TestOptionsScheduler(unittest.TestCase):
    defaults = dict(
        task_id=s.task_id,
        args=s.args,
        kwargs=s.kwargs,
        args_serializer=s.args_serializer,
        result_deserializer=s.result_deserializer,
    )

    def _get_uut(self, scheduler=None):
        from flowy.scheduler import OptionsScheduler
        if scheduler is None:
            scheduler = Mock()
        return OptionsScheduler(scheduler), scheduler

    def test_pass_trough(self):
        uut, scheduler = self._get_uut()
        self.assertEquals(uut.a, scheduler.a)

    def test_remote_activity_other_values(self):
        uut, scheduler = self._get_uut()
        result = uut.remote_activity(
            heartbeat=s.heartbeat,
            schedule_to_close=s.schedule_to_close,
            schedule_to_start=s.schedule_to_start,
            start_to_close=s.start_to_close,
            task_list=s.task_list,
            retry=s.retry,
            delay=s.delay,
            error_handling=s.error_handling,
            **self.defaults
        )
        scheduler.remote_activity.assert_called_once_with(
            heartbeat=s.heartbeat,
            schedule_to_close=s.schedule_to_close,
            schedule_to_start=s.schedule_to_start,
            start_to_close=s.start_to_close,
            task_list=s.task_list,
            retry=s.retry,
            delay=s.delay,
            error_handling=s.error_handling,
            **self.defaults
        )
        self.assertEquals(result, scheduler.remote_activity())

    def test_remote_workflow_other_values(self):
        uut, scheduler = self._get_uut()
        result = uut.remote_subworkflow(
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            task_list=s.task_list,
            retry=s.retry,
            delay=s.delay,
            error_handling=s.error_handling,
            **self.defaults
        )
        scheduler.remote_subworkflow.assert_called_once_with(
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            task_list=s.task_list,
            retry=s.retry,
            delay=s.delay,
            error_handling=s.error_handling,
            **self.defaults
        )
        self.assertEquals(result, scheduler.remote_subworkflow())

    def test_remote_activity_options_stack(self):
        uut, scheduler = self._get_uut()
        with uut.options(
            heartbeat=-5,
            schedule_to_close='10',
            schedule_to_start=15,
            start_to_close=20.1,
            workflow_duration=-1,
            decision_duration='100',
            task_list='list',
            retry=-1,
            delay='10',
            error_handling=1,
        ):
            uut.remote_activity(
                heartbeat=None,
                schedule_to_close=None,
                schedule_to_start=None,
                start_to_close=None,
                task_list=None,
                retry=None,
                delay=None,
                error_handling=True,
                **self.defaults
            )
            with uut.options():
                uut.remote_subworkflow(
                    workflow_duration=None,
                    decision_duration=None,
                    task_list=None,
                    retry=None,
                    delay=None,
                    error_handling=True,
                    **self.defaults
                )
            with uut.options(
                retry=15,
                task_list=None,
                heartbeat='5',
                decision_duration=None
            ):
                uut.remote_activity(
                    heartbeat=None,
                    schedule_to_close=None,
                    schedule_to_start=None,
                    start_to_close=None,
                    task_list=None,
                    retry=None,
                    delay=None,
                    error_handling=True,
                    **self.defaults
                )
                uut.remote_subworkflow(
                    workflow_duration=None,
                    decision_duration=None,
                    task_list=None,
                    retry=None,
                    delay=None,
                    error_handling=True,
                    **self.defaults
                )
            uut.remote_activity(
                heartbeat=None,
                schedule_to_close=None,
                schedule_to_start=None,
                start_to_close=None,
                task_list=None,
                retry=None,
                delay=None,
                error_handling=True,
                **self.defaults
            )
        uut.remote_activity(
            heartbeat=s.heartbeat,
            schedule_to_close=s.schedule_to_close,
            schedule_to_start=s.schedule_to_start,
            start_to_close=s.start_to_close,
            task_list=s.task_list,
            retry=s.retry,
            delay=s.delay,
            error_handling=s.error_handling,
            **self.defaults
        )
        scheduler.remote_activity.assert_has_calls([
            call(
                heartbeat=None,
                schedule_to_close=10,
                schedule_to_start=15,
                start_to_close=20,
                task_list='list',
                retry=0,
                delay=10,
                error_handling=True,
                **self.defaults
            ),
            call(
                heartbeat=5,
                schedule_to_close=10,
                schedule_to_start=15,
                start_to_close=20,
                task_list=None,
                retry=15,
                delay=10,
                error_handling=True,
                **self.defaults
            ),
            call(
                heartbeat=None,
                schedule_to_close=10,
                schedule_to_start=15,
                start_to_close=20,
                task_list='list',
                retry=0,
                delay=10,
                error_handling=True,
                **self.defaults
            ),
            call(
                heartbeat=s.heartbeat,
                schedule_to_close=s.schedule_to_close,
                schedule_to_start=s.schedule_to_start,
                start_to_close=s.start_to_close,
                task_list=s.task_list,
                retry=s.retry,
                delay=s.delay,
                error_handling=s.error_handling,
                **self.defaults
            ),
        ])
        scheduler.remote_subworkflow.assert_has_calls([
            call(
                workflow_duration=None,
                decision_duration=100,
                task_list='list',
                retry=0,
                delay=10,
                error_handling=True,
                **self.defaults
            ),
            call(
                workflow_duration=None,
                decision_duration=None,
                task_list=None,
                retry=15,
                delay=10,
                error_handling=True,
                **self.defaults
            ),
        ])


class TestArgsDependencyScheduler(unittest.TestCase):
    defaults = dict(
        task_id=s.id,
        result_deserializer=s.des,
        task_list=s.task_list,
        retry=s.retry,
        delay=s.delay,
        error_handling=s.error_handling,
    )

    def _get_uut(self):
        from flowy.scheduler import ArgsDependencyScheduler
        scheduler = Mock()
        return ArgsDependencyScheduler(scheduler), scheduler

    def test_pass_trough(self):
        uut, scheduler = self._get_uut()
        self.assertEquals(uut.a, scheduler.a)

    def test_activity_dispatch_to_next_scheduler(self):
        uut, scheduler = self._get_uut()
        serializer = Mock()
        scheduler.remote_activity.return_value = s.sched_value
        result = uut.remote_activity(
            args=[1, 2],
            kwargs=dict(x=1, y=2),
            args_serializer=serializer,
            heartbeat=s.heartbeat,
            schedule_to_close=s.schedule_to_close,
            schedule_to_start=s.schedule_to_start,
            start_to_close=s.start_to_close,
            **self.defaults
        )
        serializer.assert_called_once_with(1, 2, x=1, y=2)
        scheduler.remote_activity.assert_called_once_with(
            input=serializer(),
            heartbeat=s.heartbeat,
            schedule_to_close=s.schedule_to_close,
            schedule_to_start=s.schedule_to_start,
            start_to_close=s.start_to_close,
            **self.defaults
        )
        self.assertEquals(result, s.sched_value)

    def test_subworkflow_dispatch_to_next_scheduler(self):
        uut, scheduler = self._get_uut()
        serializer = Mock()
        scheduler.remote_subworkflow.return_value = s.sched_value
        result = uut.remote_subworkflow(
            args=[1, 2],
            kwargs=dict(x=1, y=2),
            args_serializer=serializer,
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            **self.defaults
        )
        serializer.assert_called_once_with(1, 2, x=1, y=2)
        scheduler.remote_subworkflow.assert_called_once_with(
            input=serializer(),
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            **self.defaults
        )
        self.assertEquals(result, s.sched_value)

    def test_placeholder_in_args(self):
        from flowy.result import Placeholder
        uut, scheduler = self._get_uut()
        result = uut.remote_subworkflow(
            args=[Placeholder(), 2],
            kwargs=dict(x=1, y=2),
            args_serializer=Mock(),
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            **self.defaults
        )
        self.assertIsInstance(result, Placeholder)

    def test_placeholder_in_kwargs(self):
        from flowy.result import Placeholder
        uut, scheduler = self._get_uut()
        result = uut.remote_subworkflow(
            args=[1, 2],
            kwargs=dict(x=Placeholder(), y=2),
            args_serializer=Mock(),
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            **self.defaults
        )
        self.assertIsInstance(result, Placeholder)

    def test_error_without_handling(self):
        from flowy.result import Error, Placeholder
        uut, scheduler = self._get_uut()
        defaults = dict(self.defaults)
        defaults['error_handling'] = False
        result = uut.remote_subworkflow(
            args=[1, Error('error')],
            kwargs=dict(x=Error('msg'), y=2),
            args_serializer=Mock(),
            workflow_duration=s.workflow_duration,
            decision_duration=s.decision_duration,
            **defaults
        )
        self.assertIsInstance(result, Placeholder)
        scheduler.fail.assert_called_once_with(reason='error\nmsg')

    def test_error_with_handling(self):
        from flowy.result import Error
        uut, scheduler = self._get_uut()
        defaults = dict(self.defaults)
        defaults['error_handling'] = True
        result = uut.remote_activity(
            args=[1, Error('error')],
            kwargs=dict(x=Error('msg'), y=2),
            args_serializer=Mock(),
            heartbeat=s.heartbeat,
            schedule_to_close=s.schedule_to_close,
            schedule_to_start=s.schedule_to_start,
            start_to_close=s.start_to_close,
            **defaults
        )
        self.assertIsInstance(result, Error)
