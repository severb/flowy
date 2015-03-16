import unittest

from flowy import LocalWorkflow
from flowy import parallel_reduce


def m(n):
    return n + 1


def r(a, b):
    return a + b


class W(object):
    def __init__(self, m, r):
        self.m = m
        self.r = r

    def __call__(self, n):
        return  parallel_reduce(self.r, map(self.m, range(n+1)))



class TestLocalWorkflow(unittest.TestCase):
    def setUp(self):
        self.sub = LocalWorkflow(W)
        self.sub.conf_activity('m', m)
        self.sub.conf_activity('r', r)

    def test_processes(self):
        main = LocalWorkflow(W)
        main.conf_workflow('m', self.sub)
        main.conf_activity('r', r)
        result = main.run(8, _wait=True)  # avoid broken pipe
        self.assertEquals(result, 165)

    def test_threads(self):
        try:
            from futures import ThreadPoolExecutor
        except ImportError:
            from concurrent.futures import ThreadPoolExecutor
        main = LocalWorkflow(W, executor=ThreadPoolExecutor)
        main.conf_workflow('m', self.sub)
        main.conf_activity('r', r)
        result = main.run(8)
        self.assertEquals(result, 165)
