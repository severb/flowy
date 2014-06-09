import unittest

from mock import Mock, ANY, call
from mock import sentinel as s


def swf_error():
    from boto.swf.exceptions import SWFResponseError
    return SWFResponseError(0, 0)


class WorkflowStarterTest(unittest.TestCase):
    def _get_uut(self):
        from flowy.swf.starter import WorkflowStarter
        client = Mock()
        wfs = WorkflowStarter(s.name, s.version, client, s.task_list, 10, 20)
        return wfs, client

    def test_calling_with_id(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        with uut.id('IMARANDOMSTRING'):
            result = uut(1, 2, x='x', y='y')

        self.assertEqual(result, 0)

        client.start_workflow_execution.assert_called_once_with(
            workflow_id='IMARANDOMSTRING',
            workflow_name='sentinel.name',
            workflow_version='sentinel.version',
            task_list='sentinel.task_list',
            execution_start_to_close_timeout='20',
            task_start_to_close_timeout='10',
            input='[[1, 2], {"y": "y", "x": "x"}]',
            tag_list=None
        )

    def test_calling_with_tags(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        with uut.tags('tag1', 'tag2'):
            result = uut(1, 2, x='x', y='y')

        self.assertEqual(result, 0)

        client.start_workflow_execution.assert_called_once_with(
            workflow_id=ANY,
            workflow_name='sentinel.name',
            workflow_version='sentinel.version',
            task_list='sentinel.task_list',
            execution_start_to_close_timeout='20',
            task_start_to_close_timeout='10',
            input='[[1, 2], {"y": "y", "x": "x"}]',
            tag_list=['tag1', 'tag2']
        )

    def test_tags_collapse(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        with uut.tags('tag1', 'tag2', 'tag1', 'tag2'):
            with uut.tags('tag1', 'tag2', 'tag1', 'tag2'):
                with uut.tags('tag1', 'tag2', 'tag1', 'tag2'):
                    result = uut(1, 2, x='x', y='y')

        self.assertEqual(result, 0)

        client.start_workflow_execution.assert_called_once_with(
            workflow_id=ANY,
            workflow_name='sentinel.name',
            workflow_version='sentinel.version',
            task_list='sentinel.task_list',
            execution_start_to_close_timeout='20',
            task_start_to_close_timeout='10',
            input='[[1, 2], {"y": "y", "x": "x"}]',
            tag_list=['tag1', 'tag2']
        )

    def test_calling_with_more_tags(self):
        uut, client = self._get_uut()
        def fails1():
            with uut.tags(*range(10)):
                pass
        def fails2():
            with uut.tags(*range(5)):
                with uut.tags(5):
                    pass
        self.assertRaises(ValueError, fails1)
        self.assertRaises(ValueError, fails2)

    def test_nesting(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        with uut.tags('tag1'):
            with uut.id('IMARANDOMSTRING1'):
                with uut.tags('tag2'):
                    with uut.id('IMARANDOMSTRING2'):
                        result1 = uut(1, 2, x='x', y='y')
                result2 = uut(1, 2, x='x', y='y')

        self.assertEqual(result1, 0)
        self.assertEqual(result2, 0)

        client.start_workflow_execution.assert_has_calls([
            call(
                workflow_id='IMARANDOMSTRING2',
                workflow_name='sentinel.name',
                workflow_version='sentinel.version',
                task_list='sentinel.task_list',
                execution_start_to_close_timeout='20',
                task_start_to_close_timeout='10',
                input='[[1, 2], {"y": "y", "x": "x"}]',
                tag_list=['tag1', 'tag2']
            ),
            call(
                workflow_id='IMARANDOMSTRING1',
                workflow_name='sentinel.name',
                workflow_version='sentinel.version',
                task_list='sentinel.task_list',
                execution_start_to_close_timeout='20',
                task_start_to_close_timeout='10',
                input='[[1, 2], {"y": "y", "x": "x"}]',
                tag_list=['tag1']
            )
        ])

    def test_calling_without_id(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        result = uut(1, 2, x='x', y='y')
        self.assertEqual(result, 0)

        client.start_workflow_execution.assert_called_once_with(
            workflow_id=ANY,
            workflow_name='sentinel.name',
            workflow_version='sentinel.version',
            task_list='sentinel.task_list',
            execution_start_to_close_timeout='20',
            task_start_to_close_timeout='10',
            input='[[1, 2], {"y": "y", "x": "x"}]',
            tag_list=None,
        )

    def test_error_response(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.side_effect = swf_error()
        result = uut(1, 2, x='x', y='y')

        self.assertIsNone(result)

