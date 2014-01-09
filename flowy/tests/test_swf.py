import unittest
from mock import create_autospec, sentinel, Mock


class AWSActivitySpecTest(unittest.TestCase):

    def _get_uut(self,
                 domain='domain',
                 name='name',
                 version='v1',
                 task_list='task_list',
                 client=None,
                 heartbeat=60,
                 schedule_to_close=420,
                 schedule_to_start=120,
                 start_to_close=300,
                 description='descr',
                 task_factory=sentinel.sentinel):

        from flowy.swf import AWSActivitySpec

        if client is None:
            from boto.swf.layer1 import Layer1
            client = create_autospec(Layer1, instance=True)

        if task_factory is sentinel.sentinel:
            task_factory = Mock()

        return AWSActivitySpec(
            domain=domain,
            name=name,
            version=version,
            task_list=task_list,
            client=client,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            description=description,
            task_factory=task_factory
        ), client, task_factory

    def test_register_fails_without_task_factory(self):
        spec, client, factory = self._get_uut(task_factory=None)
        self.assertRaises(RuntimeError, spec.register, (Mock(),))

    def test_register_passes_values_to_poller(self):
        spec, client, factory = self._get_uut(name='n', version='v')
        poller = Mock()
        spec.register(poller)
        poller.register.assert_called_once_with(
            name='n', version='v', task_factory=factory
        )

    def test_register_passes_late_bind_factory_to_poller(self):
        spec, client, factory = self._get_uut(
            name='n',
            version='v',
            task_factory=None
        )
        factory = Mock()
        poller = Mock()
        spec.bind_task_factory(factory)
        spec.register(poller)
        poller.register.assert_called_once_with(
            name='n', version='v', task_factory=factory
        )

    def test_remote_register_successful_as_new(self):
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
        result = spec.register(poller)
        client.register_activity_type.assert_called_once_with(
            domain='d',
            name='n',
            version='v',
            task_list='tl',
            default_task_heartbeat_timeout='10',
            default_task_schedule_to_close_timeout='20',
            default_task_schedule_to_start_timeout='30',
            default_task_start_to_close_timeout='40',
            description='d'
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
        from boto.swf.exceptions import SWFResponseError
        client.register_activity_type.side_effect = SWFResponseError(0, 0)
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
            domain='d',
            activity_name='n',
            activity_version='v'
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
        from boto.swf.exceptions import SWFResponseError
        client.register_activity_type.side_effect = SWFResponseError(0, 0)
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
        from boto.swf.exceptions import SWFResponseError
        client.register_activity_type.side_effect = SWFResponseError(0, 0)
        client.describe_activity_type.side_effect = SWFResponseError(0, 0)
        result = spec.register(poller)
        self.assertFalse(result)
