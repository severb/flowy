import unittest
from mock import create_autospec, Mock, call
from mock import sentinel as s


def swf_error():
    from boto.swf.exceptions import SWFResponseError
    return SWFResponseError(0, 0)


class SWFActivitySpecTest(unittest.TestCase):
    def _get_uut(self,
                 domain='domain',
                 name='name',
                 version='v1',
                 task_list='task_list',
                 client=s.sentinel,
                 heartbeat=60,
                 schedule_to_close=420,
                 schedule_to_start=120,
                 start_to_close=300,
                 description='descr',
                 task_factory=s.sentinel):
        from flowy.swf.spec import ActivitySpec
        from flowy.swf import SWFTaskId
        if client is s.sentinel:
            from boto.swf.layer1 import Layer1
            # client = create_autospec(Layer1, instance=True)
            client = Mock()    # we're using a wrapper around Layer1 so we
                               # don't have to pass domain everytime
        if task_factory is s.sentinel:
            task_factory = Mock()
        return ActivitySpec(
            task_id=SWFTaskId(name,version),
            task_list=task_list,
            client=client,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_factory=task_factory
        ), client, task_factory

    def test_register_fails_without_task_factory(self):
        spec, client, _ = self._get_uut(task_factory=None)
        self.assertRaises(RuntimeError, spec.register, Mock())

    def test_register_fails_without_client(self):
        spec, _, factory = self._get_uut(client=None)
        self.assertRaises(RuntimeError, spec.register, Mock())

    def test_register_passes_values_to_poller(self):
        from flowy.swf import SWFTaskId
        spec, client, factory = self._get_uut(name='n', version='v')
        poller = Mock()
        spec.register(poller)
        poller.register.assert_called_once_with(
            task_id=SWFTaskId(name='n', version='v'), task_factory=factory
        )

    def test_register_passes_late_bind_factory_to_poller(self):
        spec, client, factory = self._get_uut(
            name='n', version='v', task_factory=None
        )
        factory = Mock()
        poller = Mock()
        spec.bind_task_factory(factory)
        spec.register(poller)
        poller.register.assert_called_once_with(
            name='n', version='v', task_factory=factory
        )

    def test_register_uses_late_bind_client(self):
        spec, _, factory = self._get_uut(client=None)
        client = Mock()
        poller = Mock()
        spec.bind_client(client)
        spec.register(poller)
        self.assertTrue(client.register_activity_type.called)

    def test_factory_must_be_callable(self):
        spec, client, factory = self._get_uut(task_factory=None)
        self.assertRaises(ValueError, spec.bind_task_factory, 'not callable')

    def test_remote_register_successful_as_new(self):
        spec, client, factory = self._get_uut(
            name='n',
            version='v',
            task_list='tl',
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
        )
        poller = Mock()
        result = spec.register(poller)
        client.register_activity_type.assert_called_once_with(
            name='n',
            version='v',
            task_list='tl',
            default_task_heartbeat_timeout='10',
            default_task_schedule_to_close_timeout='20',
            default_task_schedule_to_start_timeout='30',
            default_task_start_to_close_timeout='40',
        )
        self.assertTrue(result)

    def test_remote_register_checks_compatibility_success(self):
        spec, client, factory = self._get_uut(
            domain='d',
            name='n',
            version='v',
            task_list='tl',
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            description='d'
        )
        poller = Mock()
        client.register_activity_type.side_effect = swf_error()
        client.describe_activity_type.return_value = {
            'configuration': {
                'defaultTaskList': {'name': 'tl'},
                'defaultTaskHeartbeatTimeout': '10',
                'defaultTaskScheduleToCloseTimeout': '20',
                'defaultTaskScheduleToStartTimeout': '30',
                'defaultTaskStartToCloseTimeout': '40'
            }
        }
        result = spec.register(poller)
        client.describe_activity_type.assert_called_once_with(
            activity_name='n', activity_version='v'
        )
        self.assertTrue(result)

    def test_remote_register_checks_compatibility_different(self):
        spec, client, factory = self._get_uut(
            task_list='tl',
            heartbeat=10,
            schedule_to_close=20,
            schedule_to_start=30,
            start_to_close=40,
            description='d'
        )
        poller = Mock()
        client.register_activity_type.side_effect = swf_error()
        client.describe_activity_type.return_value = {
            'configuration': {
                'defaultTaskList': {'name': 'tl_'},
                'defaultTaskHeartbeatTimeout': '100',
                'defaultTaskScheduleToCloseTimeout': '200',
                'defaultTaskScheduleToStartTimeout': '300',
                'defaultTaskStartToCloseTimeout': '400'
            }
        }
        result = spec.register(poller)
        self.assertFalse(result)

    def test_remote_register_checks_compatibility_fails(self):
        spec, client, factory = self._get_uut()
        poller = Mock()
        client.register_activity_type.side_effect = swf_error()
        client.describe_activity_type.side_effect = swf_error()
        result = spec.register(poller)
        self.assertFalse(result)


class SWFWorkflowSpecTest(unittest.TestCase):
    def _get_uut(self,
                 name='name',
                 version='version',
                 task_list='task_list',
                 client=None,
                 workflow_duration='1',
                 decision_duration='2',
                 task_factory=s.sentinel):
        from flowy.swf.spec import WorkflowSpec
        from flowy.swf import SWFTaskId
        if client is None:
            from boto.swf.layer1 import Layer1
            # client = create_autospec(Layer1, instance=True)
            client = Mock()    # we're using a wrapper around Layer1 so we
                               # don't have to pass domain everytime
        if task_factory is s.sentinel:
            task_factory = Mock()
        return WorkflowSpec(
            task_id=SWFTaskId(name, version),
            task_list=task_list,
            client=client,
            workflow_duration=workflow_duration,
            decision_duration=decision_duration,
            task_factory=task_factory
        ), client, task_factory

    def test_remote_register(self):
        spec, client, task_factory = self._get_uut(
            name='n',
            version='v',
            task_list='tl',
            workflow_duration=10,
            decision_duration=20,
        )
        spec.register(Mock())
        client.register_workflow_type.assert_called_once_with(
            name='n',
            version='v',
            task_list='tl',
            default_execution_start_to_close_timeout=10,
            default_task_start_to_close_timeout=20,
        )

    def test_remote_checks_compatibility(self):
        spec, client, task_factory = self._get_uut(
            name='n', version='v'
        )
        client.register_workflow_type.side_effect = swf_error()
        client.describe_workflow_type.return_value = {'configuration': {
                'defaultTaskList': {'name':'aaa'},
                'defaultExecutionStartToCloseTimeout': 10,
                'defaultTaskStartToCloseTimeout': 30
            }
        }
        spec.register(Mock())
        client.describe_workflow_type.assert_called_once_with(
            workflow_name='n', workflow_version='v'
        )

    def test_remote_check_compatibility_fails(self):
        spec, client, task_factory = self._get_uut(
            name='n', version='v'
        )
        client.register_workflow_type.side_effect = swf_error()
        client.describe_workflow_type.side_effect = swf_error()
        result = spec.register(Mock())
        self.assertFalse(result)


class RemoteCollectorSpecTest(unittest.TestCase):
    def _get_uut(self, factory=None):
        from flowy.spec import RemoteCollectorSpec
        if factory is None:
            factory = Mock()
        return RemoteCollectorSpec(spec_factory=factory), factory

    def test_register_empty(self):
        spec, factory = self._get_uut()
        poller = Mock()
        result = spec.register(poller)
        self.assertTrue(result)
        self.assertFalse(poller.register.called)

    def test_register_detected(self):
        spec, factory = self._get_uut()
        poller = Mock()
        spec.detect(s.f1, s.x, a=s.a, b=s.b)
        spec.detect(s.f2, s.y, s.z, aa=s.aa, bb=s.bb)
        factory.assert_has_calls([
            call(s.x, a=s.a, b=s.b),
            call().bind_task_factory(s.f1),
            call(s.y, s.z, aa=s.aa, bb=s.bb),
            call().bind_task_factory(s.f2),
        ])
        result = spec.register(poller)
        self.assertTrue(result)
        factory.assert_has_calls([
            call().register(poller),
            call().register(poller)
        ])

    def test_register_fails(self):
        spec, factory = self._get_uut()
        factory().register.side_effect = True, False
        poller = Mock()
        spec.detect(s.x, a=s.a)
        spec.detect(s.y, s.z)
        result = spec.register(poller)
        self.assertFalse(result)

    def test_bind_client(self):
        spec, factory = self._get_uut()
        spec.detect(s.x, a=s.a)
        spec.detect(s.y, s.z)
        spec.bind_client(s.c)
        factory().bind_client.assert_has_calls([call(s.c), call(s.c)])


class RemoteScannerSpecTest(unittest.TestCase):
    def _get_uut(self, collector=None):
        from flowy.spec import RemoteScannerSpec
        if collector is None:
            collector = Mock()
        return RemoteScannerSpec(collector=collector), collector

    def test_decorator_functionality(self):
        scanner, collector = self._get_uut()
        wrapped = scanner(s.x, s.y, kw1=s.a, kw2=s.b)(s.fact)
        self.assertIs(wrapped, s.fact)
        collector.detect.assert_called_once_with(
            s.fact, s.x, s.y, kw1=s.a, kw2=s.b
        )

    def test_forward_register_call(self):
        scanner, collector = self._get_uut()
        scanner.register(s.fact)
        collector.register.assert_called_once_with(s.fact)

    def test_bind_client(self):
        scanner, collector = self._get_uut()
        scanner.bind_client(s.c)
        collector.bind_client.assert_called_once_with(s.c)
