import unittest

from mock import sentinel as s
from mock import Mock, call, patch


class TestRegistrationDecorators(unittest.TestCase):

    def _get_uut(self):
        from flowy.scanner import Scanner
        registry = Mock()
        return Scanner(registry=registry), registry

    def test_activity_detection(self):
        uut, registry = self._get_uut()

        import flowy.tests.specs
        from flowy.spec import SWFActivitySpec, SWFWorkflowSpec

        uut.scan_activities(package=flowy.tests.specs)

        registry.add.assert_has_calls([
            call(SWFActivitySpec(
                name='a1',
                version=s.version,
                task_list='sentinel.task_list',
                heartbeat=300,
                schedule_to_close=420,
                schedule_to_start=120,
                start_to_close=300),
                s.task_factory1),
            call(SWFActivitySpec(
                version=s.version,
                task_list='sentinel.task_list',
                heartbeat='6',
                schedule_to_close=42.0,
                schedule_to_start=12,
                start_to_close=30,
                name='activity2'),
                s.task_factory2)
        ])
        registry.reset_mock()

        uut.scan_workflows(package=flowy.tests.specs)
        print 'aaa', registry.method_calls

        registry.add.assert_has_calls([
            call(SWFWorkflowSpec(
                name='w1',
                version=s.version,
                task_list=s.task_list,
                workflow_duration=3600,
                decision_duration=60),
                s.task_factory3,
                ),
            call(SWFWorkflowSpec(
                name='w2',
                version=s.version,
                task_list=s.task_list,
                workflow_duration=120,
                decision_duration=5),
                s.task_factory4,
                ),
            ])

    @patch('venusian.Scanner')
    def test_default_package(self, mock_scanner):
        import flowy.tests
        uut, _ = self._get_uut()
        uut.scan_activities()
        mock_scanner().scan.assert_called_once_with(
            flowy.tests, categories=('activity',), ignore=None
        )


class TestSWFTaskRegistry(unittest.TestCase):
    def _get_uut(self):
        from flowy.scanner import SWFTaskRegistry
        spec = Mock()
        factory = Mock()
        return SWFTaskRegistry(), spec, factory

    def test_adding_spec(self):
        reg, spec, factory = self._get_uut()
        reg.add(spec, factory)
        self.assertDictEqual(reg._registry, {spec: factory})

    def test_call_inexistent(self):
        reg, spec, factory = self._get_uut()
        reg.add(spec, factory)
        ret = reg('inexistent')
        self.assertIsNone(ret())

    def test_remote_register_success(self):
        reg, spec, factory = self._get_uut()
        spec.register_remote.return_value = True
        reg.add(spec, factory)
        swf_client = Mock()
        ret = reg.register_remote(swf_client)
        self.assertEqual(ret, [])
        spec.register_remote.assert_called_once_with(swf_client)

    def test_remote_register_failure(self):
        reg, spec, factory = self._get_uut()
        spec.register_remote.return_value = False
        reg.add(spec, factory)
        swf_client = Mock()
        ret = reg.register_remote(swf_client)
        self.assertEqual(ret, [spec])
        spec.register_remote.assert_called_once_with(swf_client)


