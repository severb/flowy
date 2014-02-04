import unittest

from flowy.swf import SWFTaskId


class TestSWFTasks(unittest.TestCase):

    def test_activity_proxy(self):
        from flowy.swf.task import ActivityProxy
        uut = ActivityProxy('name', 'version')
        self.assertEquals(uut._kwargs['task_id'], SWFTaskId('name', 'version'))

    def test_workflow_proxy(self):
        from flowy.swf.task import WorkflowProxy
        uut = WorkflowProxy('name', 'version')
        self.assertEquals(uut._kwargs['task_id'], SWFTaskId('name', 'version'))
