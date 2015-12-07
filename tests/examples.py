#!/usr/bin/env python
from __future__ import print_function

import argparse
import logging
import random
import time

from flowy import finish_order, first, LocalWorkflow, parallel_reduce, wait
from flowy.utils import setup_default_logger


def activity(x=None, y=None, identity=None, sleep=None, err=None):
    """Simulate an activity that actually does something.

    If only one argument is received, compute its square, else sum the two
    arguments.
    """
    if sleep is None:
        sleep = random.random()
    print('Start activity (sleep %fs): %s' % (sleep, identity))
    time.sleep(sleep)
    print('Finish activity (sleep %fs): %s' % (sleep, identity))
    if err is not None:
        raise RuntimeError(err)
    if y is None:
        return x * x
    return x + y


class Sequential(object):
    """Chain activity calls.

    10 ---------4|
     5           ---16|
    15                ------------256|
  R 20                               ---------------65536
    Duration: 50
    Result: 0
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        r = 2
        t = map(lambda x: x * time_scale, [1.0, 0.5, 1.5, 2.0])
        for identity, sleep in enumerate(t, 1):
            r = self.a(r, sleep=sleep, identity=identity)
        return r


class ParallelWait(object):
    """Parallelize activity calls and wait for all to finish.

    Execution digram for n=4
    10 ---------1----|
     5 ----4---------|
  R 15 --------------9
    20 --------------|---16
    Duration: 20
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        results = []
        t = map(lambda x: x * time_scale, [1.0, 0.5, 1.5, 2.0])
        for i, sleep in enumerate(t, 1):
            results.append(self.a(i, identity=i, sleep=sleep))
        for result in results:
            wait(result)
        return results[-2]


class ParallelSum(object):
    """Similar with ParallelWait but instead of just waiting, compute the sum
    of the results. The wait here is implicit.

    10 ---------1----------|
  R  0                     30
     5 ----4---------------|
    15 --------------9-----|
    20 ------------------16|
    Duration: 20
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        results = []
        t = map(lambda x: x * time_scale, [1.0, 0.5, 1.5, 2.0])
        for i, sleep in enumerate(t, 1):
            results.append(self.a(i, identity=i, sleep=sleep))
        return sum(results)


class WaitFirst(object):
    """Parallelize activity calls and wait for the first to finish, ignoring
    the others.
    10 ---------1
  R  5 ----4
    15 --------------9
    20 ------------------16
    Duration: 5
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        x1 = self.a(1, sleep=1.0 * time_scale, identity=1)
        x2 = self.a(2, sleep=0.5 * time_scale, identity=2)
        x3 = self.a(3, sleep=1.5 * time_scale, identity=3)
        x4 = self.a(4, sleep=2.0 * time_scale, identity=4)
        return first(x1, x2, x3, x4)


class WaitFirstTwo(object):
    """Parallelize activity calls and wait for the first two activities to
    finish, ignoring the others.
    10 ---------1|
  R  0           5
     5 ----4-----|
    15 --------------9
    20 ------------------16
    Duration: 10
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        x1 = self.a(1, sleep=1.0 * time_scale, identity=1)
        x2 = self.a(2, sleep=0.5 * time_scale, identity=2)
        x3 = self.a(3, sleep=1.5 * time_scale, identity=3)
        x4 = self.a(4, sleep=2.0 * time_scale, identity=4)
        f_o = list(finish_order(x1, x2, x3, x4))
        return f_o[0] + f_o[1]
        # Alternatively, if the result is not important, just wait
        # f_o = finish_order(x1, x2, x3, x4)
        # wait(next(f_o))
        # wait(next(f_o))


class Conditional(object):
    """Start an activity conditioned by the result of a previous activity.

    10 -------100|
  R 10           -------400
    Duration: 20
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        x = self.a(10, identity=1, sleep=1.0 * time_scale)
        if x >= 100:
            return self.a(20, identity='in if', sleep=1.0 * time_scale)
        return 1


class NaiveMapReduce(object):
    """A naive map reduce implementation.
    This works for non associative and non commutative reduce functions.
    No reduce operation can take place in parallel.

     5 ----1----------|
    15                --------------5|
    15                |              -------------14---------------|
    15                |              |                             -------------30|
    15                |              |                             |              -------------55|
  R 15                |              |                             |              |              -------------91
    15 --------------4|              |                             |              |              |
    10 ---------9--------------------|                             |              |              |
    60 ----------------------------------------------------------16|              |              |
    50 ------------------------------------------------25-------------------------|              |
    20 ------------------36----------------------------------------------------------------------|
    Duration: 105
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        reduce_f = lambda x, y: self.a(x, y,
                                       sleep=1.5 * time_scale,
                                       identity='reduce %s %s' % (x, y))
        t = map(lambda x: x * time_scale, [0.5, 1.5, 1.0, 6.0, 5.0, 2.0])
        map_f = lambda x, sleep: self.a(x, sleep=sleep, identity='map %s' % x)
        results = list(map(map_f, range(1, 7), t))
        v = results[0]
        for r in results[1:]:
            v = reduce_f(v, r)
        return v


class FinishOrderMapReduce(object):
    """A better map reduce implementation.
    The reduction is started as soon as any 2 map operations are completed, and
    continues when the last reduce and a new map operation are completed.
    No reduce operation can take place in parallel.

     5 ----1-----|
    15           -------------10|
    15           |              -------------14|
    15           |              |              -------------50|
    15           |              |              |              -------------75|
  R 15           |              |              |              |              -------------91
    15 ----------|---4----------|              |              |              |
    10 ---------9|                             |              |              |
    60 ----------------------------------------|--------------|--16----------|
    50 ----------------------------------------|-------25-----|
    20 ------------------36--------------------|
    Duration: 85
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        reduce_f = lambda x, y: self.a(x, y,
                                       sleep=1.5 * time_scale,
                                       identity='reduce %s %s' % (x, y))
        t = map(lambda x: x * time_scale, [0.5, 1.5, 1.0, 6.0, 5.0, 2.0])
        map_f = lambda x, sleep: self.a(x, sleep=sleep, identity='map %s' % x)
        results = finish_order(map(map_f, range(1, 7), t))
        v = next(results)
        for r in results:
            v = reduce_f(v, r)
        return v


class ParallelMapReduce(object):
    """A parallel map reduce implementation.
    The reduction is started as soon as any 2 tasks are completed, including
    previous reductions.
    The reduce operations are parallelized.

     5 ----1-----|
    15           -------------10----------|
    15           |                        -------------50|
    15           |                        |              -------------75|
  R 15           |                        |              |              -------------91
    15 ----------|---4-----|              |              |              |
    15           |         -------------40|              |              |
    10 ---------9|         |                             |              |
    60 --------------------|-----------------------------|-------16-----|
    50 --------------------|---------------------------25|
    20 ------------------36|
    Duration: 80
    """

    def __init__(self, a):
        self.a = a

    def __call__(self, time_scale=1):
        reduce_f = lambda x, y: self.a(x, y,
                                       sleep=1.5 * time_scale,
                                       identity='reduce %s %s' % (x, y))
        t = map(lambda x: x * time_scale, [0.5, 1.5, 1.0, 6.0, 5.0, 2.0])
        map_f = lambda x, sleep: self.a(x, sleep=sleep, identity='map %s' % x)
        results = map(map_f, range(1, 7), t)
        return parallel_reduce(reduce_f, results)


def main():
    # logging, the basicConfig is for futures, setup_default_logger is for flowy
    logging.basicConfig()
    setup_default_logger()

    def workflow(workflow_class):
        try:
            return globals()[workflow_class]
        except KeyError:
            raise ValueError('Workflow "%s" not found.' % workflow_class)

    parser = argparse.ArgumentParser(description='Example workflow runner.')
    parser.add_argument('workflow', action='store', type=workflow)
    parser.add_argument('--pure', action='store_true', default=False)
    parser.add_argument('--workflow-workers',
                        action='store',
                        type=int,
                        default=2)
    parser.add_argument('--activity-workers',
                        action='store',
                        type=int,
                        default=8)
    parser.add_argument('--timeit', action='store_true', default=False)
    parser.add_argument('--trace', action='store_true', default=False)
    parser.add_argument('--wait-children', action='store_true', default=False)
    args = parser.parse_args()

    start = time.time()
    if args.pure:
        wf = args.workflow(activity)
        result = wf()
    else:
        lw = LocalWorkflow(args.workflow,
                           activity_workers=args.activity_workers,
                           workflow_workers=args.workflow_workers)
        lw.conf_activity('a', activity)
        result = lw.run(_wait=args.wait_children, _trace=args.trace)
    if args.timeit:
        print('Timed at:', time.time() - start)
    print('Result:', result)


if __name__ == '__main__':
    main()
