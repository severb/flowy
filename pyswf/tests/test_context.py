import unittest


class TestWorkflowContext(unittest.TestCase):

    def _get_uut(self):
        from pyswf.context import WorkflowContext2
        return WorkflowContext2()

    def test_activity_scheduled(self):
        workflow = self._get_uut()
        self.assertFalse(workflow.is_activity_scheduled('act1234'))
        workflow.set_scheduled('act1234', 23)
        self.assertTrue(workflow.is_activity_scheduled('act1234'))

    def test_activity_result(self):
        workflow = self._get_uut()
        workflow.set_scheduled('act1234', 'a1')
        self.assertFalse(workflow.activity_result('act1234', default=False))
        workflow.set_result('a1', 'amazing_result')
        self.assertEquals(
            'amazing_result',
            workflow.activity_result('act1234')
        )

    def test_activity_timed_out(self):
        workflow = self._get_uut()
        workflow.set_scheduled('act1234', 'a1')
        self.assertFalse(workflow.is_activity_timeout('act1234'))
        workflow.set_timed_out('a1')
        self.assertTrue(workflow.is_activity_timeout('act1234'))

    def test_activity_error(self):
        workflow = self._get_uut()
        workflow.set_scheduled('act1234', 'a1')
        self.assertFalse(workflow.is_activity_result_error('act1234'))
        workflow.set_error('a1', 'some error')
        self.assertTrue(workflow.is_activity_result_error('act1234'))
