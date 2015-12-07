import inspect
import time
import unittest
from functools import partial

from flowy import LocalWorkflow
from flowy import TaskError
from flowy import parallel_reduce
from flowy import restart

try:
    from concurrent.futures import ThreadPoolExecutor
except ImportError:
    from futures import ThreadPoolExecutor


def tactivity(a=None, b=None, err=None):
    if a is not None and b is not None:
        result = a + b
    elif a is not None:
        result = a + 1
    if err is not None:
        raise RuntimeError(err)
    return result


class TWorkflow(object):
    def __call__(self, a=None, b=None, err=None, r=0):
        if r:
            return restart(a, b, err, r=r-1)
        return tactivity(a, b, err)


class W(object):
    def __init__(self, m, r):
        self.m = m
        self.r = r

    def __call__(self, n, r=True):
        if r:
            return restart(n, r=False)
        return parallel_reduce(self.r, map(self.m, range(n + 1)))


class F(object):
    def __init__(self, task):
        self.task = task

    def __call__(self, r=0, throw=False):
        if r:
            return restart(r=r-1)
        if throw:
            raise ValueError('Err!')
        return self.task(err='Err!')


class TestLocalWorkflow(unittest.TestCase):
    def test_activities_processes(self):
        main = LocalWorkflow(W)
        main.conf_activity('m', tactivity)
        main.conf_activity('r', tactivity)
        result = main.run(8, _wait=True)  # avoid broken pipe
        self.assertEquals(result, 45)

    def test_activities_threads(self):
        try:
            from futures import ThreadPoolExecutor
        except ImportError:
            from concurrent.futures import ThreadPoolExecutor
        main = LocalWorkflow(W, executor=ThreadPoolExecutor)
        main.conf_activity('m', tactivity)
        main.conf_activity('r', tactivity)
        result = main.run(8, r=True, _wait=True)
        self.assertEquals(result, 45)

    def test_subworkflows_processes(self):
        sub = LocalWorkflow(TWorkflow)
        main = LocalWorkflow(W)
        main.conf_workflow('m', sub)
        main.conf_workflow('r', sub)
        result = main.run(8, r=True, _wait=True)
        self.assertEquals(result, 45)

    def test_subworkflows_threads(self):
        try:
            from futures import ThreadPoolExecutor
        except ImportError:
            from concurrent.futures import ThreadPoolExecutor
        sub = LocalWorkflow(TWorkflow)
        main = LocalWorkflow(W, executor=ThreadPoolExecutor)
        main.conf_workflow('m', sub)
        main.conf_workflow('r', sub)
        result = main.run(8, r=True, _wait=True)
        self.assertEquals(result, 45)

    def test_selfsubworkflows_threads(self):
        try:
            from futures import ThreadPoolExecutor
        except ImportError:
            from concurrent.futures import ThreadPoolExecutor
        sub = LocalWorkflow(W, executor=ThreadPoolExecutor)
        sub.conf_activity('m', tactivity)
        sub.conf_activity('r', tactivity)
        main = LocalWorkflow(W, executor=ThreadPoolExecutor)
        main.conf_workflow('m', sub)
        main.conf_activity('r', tactivity)
        result = main.run(8, r=True, _wait=True)
        self.assertEquals(result, 165)

    def test_selfsubworkflows_processes(self):
        sub = LocalWorkflow(W, executor=ThreadPoolExecutor)
        sub.conf_activity('m', tactivity)
        sub.conf_activity('r', tactivity)
        main = LocalWorkflow(W)
        main.conf_workflow('m', sub)
        main.conf_activity('r', tactivity)
        result = main.run(8, r=True, _wait=True)
        self.assertEquals(result, 165)

    def test_fail_activity(self):
        main = LocalWorkflow(F)
        main.conf_activity('task', tactivity)
        self.assertRaises(TaskError, lambda: main.run(_wait=True))
        main = LocalWorkflow(F)
        main.conf_activity('task', tactivity)
        self.assertRaises(TaskError, lambda: main.run(r=1, _wait=True))
        main = LocalWorkflow(F)
        main.conf_activity('task', tactivity)
        self.assertRaises(TaskError, lambda: main.run(r=4, _wait=True))
        main = LocalWorkflow(F)
        main.conf_activity('task', tactivity)
        self.assertRaises(TaskError, lambda: main.run(throw=True, _wait=True))

    def test_fail_subworkflow(self):
        main = LocalWorkflow(F)
        sub = LocalWorkflow(TWorkflow)
        main.conf_workflow('task', sub)
        self.assertRaises(TaskError, lambda: main.run(_wait=True))
        main = LocalWorkflow(F)
        main.conf_workflow('task', sub)
        self.assertRaises(TaskError, lambda: main.run(r=1, _wait=True))
        main = LocalWorkflow(F)
        main.conf_workflow('task', sub)
        self.assertRaises(TaskError, lambda: main.run(r=4, _wait=True))
        main = LocalWorkflow(F)
        main.conf_workflow('task', sub)
        self.assertRaises(TaskError, lambda: main.run(throw=True, _wait=True))


class TestExamples(unittest.TestCase):
    """Since there are time assertions, this tests can generate false
    positives. Changing TIME_SCALE to 1 should fix most of the problems but
    will significantly increase the tests duration."""
    pass


TIME_SCALE = 0.1


def make_t(wf_name, wf):
    def test(self):
        lw = LocalWorkflow(wf,
                           activity_workers=16,
                           workflow_workers=2,
                           executor=ThreadPoolExecutor)
        lw.conf_activity('a', examples.activity)
        start = time.time()
        result = lw.run(TIME_SCALE)
        duration = time.time() - start
        lines = [l.strip() for l in wf.__doc__.split("\n")]
        expected = None
        for line in lines:
            if line.startswith('R '):
                expected = int(line.split()[-1].split("-")[-1])
                break
        self.assertEquals(expected, result)
        for line in lines:
            if line.startswith('Duration:'):
                expected_duration = int(line.split()[-1]) * TIME_SCALE * 0.1
                break
        print(expected_duration, duration)
        self.assertTrue(abs(expected_duration - duration) < TIME_SCALE * 0.9)

    return test


import examples
for wf_name, wf in vars(examples).items():
    if inspect.isclass(wf) and wf.__module__ == 'flowy.examples':
        setattr(TestExamples, 'test_%s' % wf_name, make_t(wf_name, wf))
