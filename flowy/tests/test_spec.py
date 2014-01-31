import unittest

from mock import sentinel as s
from mock import Mock, call


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


class TestActivitySpecCollector(unittest.TestCase):
    def _get_uut(self):
        from flowy.spec import ActivitySpecCollector
        factory = Mock()
        return (
            ActivitySpecCollector(spec_factory=factory, client=s.client),
            factory
        )

    def test_empty_register(self):
        uut, factory = self._get_uut()
        uut.register(s.worker)
        self.assertEquals(factory.call_count, 0)

    def test_collect(self):
        uut, factory = self._get_uut()
        uut.collect(
            task_id=s.task_id1,
            task_factory=s.task_factory1,
            task_list=s.task_list1,
            heartbeat=s.heartbeat1,
            schedule_to_close=s.schedule_to_close1,
            schedule_to_start=s.schedule_to_start1,
            start_to_close=s.start_to_close1
        )
        uut.collect(
            task_id=s.task_id2,
            task_factory=s.task_factory2,
            task_list=s.task_list2,
            heartbeat=s.heartbeat2,
            schedule_to_close=s.schedule_to_close2,
            schedule_to_start=s.schedule_to_start2,
            start_to_close=s.start_to_close2
        )
        factory.assert_has_calls([
            call(
                task_id=s.task_id1,
                task_factory=s.task_factory1,
                client=s.client,
                task_list=s.task_list1,
                heartbeat=s.heartbeat1,
                schedule_to_close=s.schedule_to_close1,
                schedule_to_start=s.schedule_to_start1,
                start_to_close=s.start_to_close1
            ),
            call(
                task_id=s.task_id2,
                task_factory=s.task_factory2,
                client=s.client,
                task_list=s.task_list2,
                heartbeat=s.heartbeat2,
                schedule_to_close=s.schedule_to_close2,
                schedule_to_start=s.schedule_to_start2,
                start_to_close=s.start_to_close2
            )
        ])
        factory().register.side_effect = True, False
        result = uut.register(s.worker)
        factory().register.assert_has_calls([call(s.worker), call(s.worker)])
        self.assertEquals(len(result), 1)


class TestWorkflowSpecCollector(unittest.TestCase):
    def _get_uut(self):
        from flowy.spec import WorkflowSpecCollector
        factory = Mock()
        return (
            WorkflowSpecCollector(spec_factory=factory, client=s.client),
            factory
        )

    def test_collect(self):
        uut, factory = self._get_uut()
        uut.collect(
            task_id=s.task_id1,
            task_factory=s.task_factory1,
            task_list=s.task_list1,
            decision_duration=s.decision_duration,
            workflow_duration=s.workflow_duration
        )
        factory.assert_called_once_with(
            task_id=s.task_id1,
            task_factory=s.task_factory1,
            client=s.client,
            task_list=s.task_list1,
            decision_duration=s.decision_duration,
            workflow_duration=s.workflow_duration
        )
        uut.register(s.worker)
        factory().register.assert_called_once_with(s.worker)
