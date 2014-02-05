import unittest

from mock import patch

from flowy.swf import SWFTaskId


class ScannerTest(unittest.TestCase):

    @patch('flowy.swf.scanner.a')
    def test_activity(self, a):
        from flowy.swf.scanner import activity

        activity('name', 'v1', 'tl')
        a.assert_called_once_with(task_id=SWFTaskId('name', 'v1'),
                                  task_list='tl')

    @patch('flowy.swf.scanner.w')
    def test_workflow(self, w):
        from flowy.swf.scanner import workflow

        workflow('name', 'v1', 'tl')
        w.assert_called_once_with(task_id=SWFTaskId('name', 'v1'),
                                  task_list='tl')
