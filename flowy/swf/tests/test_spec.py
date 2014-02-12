import unittest
from mock import Mock
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
            client = Mock()
        if task_factory is s.sentinel:
            task_factory = Mock()
        return ActivitySpec(
            task_id=SWFTaskId(name, version),
            task_list=task_list,
            client=client,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_factory=task_factory
        ), client, task_factory

    def test_register_passes_values_to_poller(self):
        from flowy.swf import SWFTaskId
        spec, client, factory = self._get_uut(name='n', version='v')
        poller = Mock()
        spec.register(poller)
        poller.register.assert_called_once_with(
            task_id=SWFTaskId(name='n', version='v'), task_factory=factory
        )

    def test_register_skip_worker_registration(self):
        spec, client, factory = self._get_uut(name='n', version='v')
        spec.register(worker=None)
        client.register_activity_type.assert_called_once_with(
            name='n',
            version='v',
            task_list='task_list',
            default_task_heartbeat_timeout='60',
            default_task_schedule_to_close_timeout='420',
            default_task_schedule_to_start_timeout='120',
            default_task_start_to_close_timeout='300',
        )

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
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        for err in [swf_error(), SWFTypeAlreadyExistsError(0, 0)]:
            client.register_activity_type.side_effect = err
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
            client.describe_activity_type.assert_called_with(
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
            client = Mock()
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
            default_execution_start_to_close_timeout='10',
            default_task_start_to_close_timeout='20',
            default_child_policy='TERMINATE',
        )

    def test_remote_checks_compatibility(self):
        spec, client, task_factory = self._get_uut(
            name='n', version='v'
        )
        from boto.swf.exceptions import SWFTypeAlreadyExistsError
        for err in [swf_error(), SWFTypeAlreadyExistsError(0, 0)]:
            client.register_workflow_type.side_effect = err
            client.describe_workflow_type.return_value = {
                'configuration': {
                    'defaultTaskList': {'name': 'aaa'},
                    'defaultExecutionStartToCloseTimeout': 10,
                    'defaultTaskStartToCloseTimeout': 30
                }
            }
            spec.register(Mock())
            client.describe_workflow_type.assert_called_with(
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


class ActivitySpecCollector(unittest.TestCase):
    def _get_uut(self, factory=None, client=s.client):
        from flowy.spec import ActivitySpecCollector
        if factory is None:
            factory = Mock()
        return ActivitySpecCollector(factory, client), factory

    def test_register_empty(self):
        spec, factory = self._get_uut()
        poller = Mock()
        result = spec.register(poller)
        self.assertEquals(result, [])
        self.assertFalse(poller.register.called)


class RemoteScannerSpecTest(unittest.TestCase):
    def _get_uut(self, collector=None):
        from flowy.scanner import Scanner
        if collector is None:
            collector = Mock()
        return Scanner(collector=collector), collector

    def test_forward_register_call(self):
        scanner, collector = self._get_uut()
        scanner.register(s.fact)
        collector.register.assert_called_once_with(s.fact)
