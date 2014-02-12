import unittest

from mock import Mock, ANY
from mock import sentinel as s


def swf_error():
    from boto.swf.exceptions import SWFResponseError
    return SWFResponseError(0, 0)


class WorkflowStarterTest(unittest.TestCase):
    def _get_uut(self):
        from flowy.swf.starter import WorkflowStarter
        client = Mock()
        wfs = WorkflowStarter(s.domain, s.name, s.version, s.task_list,
                              10, 20, client=client)
        return wfs, client

    def test_calling_with_id(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        result = uut.with_id('IMARANDOMSTRING')(1, 2, x='x', y='y')

        self.assertEqual(result, 0)

        client.start_workflow_execution.assert_called_once_with(
            domain='sentinel.domain',
            workflow_id='IMARANDOMSTRING',
            workflow_name='sentinel.name',
            workflow_version='sentinel.version',
            task_list='sentinel.task_list',
            execution_start_to_close_timeout='20',
            task_start_to_close_timeout='10',
            input='[[1, 2], {"y": "y", "x": "x"}]'
        )

    def test_calling_without_id(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.return_value = {'runId': 0}
        result = uut(1, 2, x='x', y='y')
        self.assertEqual(result, 0)

        client.start_workflow_execution.assert_called_once_with(
            domain='sentinel.domain',
            workflow_id=ANY,
            workflow_name='sentinel.name',
            workflow_version='sentinel.version',
            task_list='sentinel.task_list',
            execution_start_to_close_timeout='20',
            task_start_to_close_timeout='10',
            input='[[1, 2], {"y": "y", "x": "x"}]'
        )

    def test_error_response(self):
        uut, client = self._get_uut()
        client.start_workflow_execution.side_effect = swf_error()
        result = uut(1, 2, x='x', y='y')

        self.assertIsNone(result)

