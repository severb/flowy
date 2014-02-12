import unittest
from flowy.exception import SuspendTask, TaskError, TaskTimedout


class ResultTest(unittest.TestCase):

    def test_placeholder(self):
        from flowy.result import Placeholder

        res = Placeholder()
        self.assertRaises(SuspendTask, res.result)

    def test_error(self):
        from flowy.result import Error

        res = Error('reason')
        self.assertRaisesRegexp(TaskError, 'reason', res.result)

    def test_timeout(self):
        from flowy.result import Timeout

        res = Timeout()
        self.assertRaises(TaskTimedout, res.result)

    def test_result(self):
        from flowy.result import Result

        res = Result('reason')
        self.assertEquals('reason', res.result())
