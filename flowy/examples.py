from __future__ import print_function

import argparse
import logging
import random
import time

from flowy import finish_order, first, LocalWorkflow, parallel_reduce, wait
from flowy.base import setup_default_logger


# logging, the basicConfig is for futures, setup_default_logger is for flowy
logging.basicConfig()
setup_default_logger()


def activity(x=None, y=None, identity=None, result=None, sleep=None):
    """Simulate an activity that actually does something."""
    # x and y are needed only so we can fake passing arguments ot this activity
    if sleep is None:
        sleep = random.random()
    print('Start activity (sleep %fs): %s' % (sleep, identity))
    time.sleep(sleep)
    print('Finish activity (sleep %fs): %s' % (sleep, identity))
    if result == 'random':
        return random.random()
    return result


class Sequential(object):
    """Chain activity calls.

    10 ---------1|
     5           ----2|
    15                --------------3|
  R 20                               -------------------4
    Duration: 50
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        r = 0
        for identity, sleep in enumerate([1.0, 0.5, 1.5, 2.0], 1):
            r = self.a(sleep=sleep, result=r+1, identity=identity)
        return r

class ParallelWait(object):
    """Parallelize activity calls and wait for all to finish.

    Execution digram for n=4
    10 ---------1----------|
  R  0                     None
     5 ----2---------------|
    15 --------------3-----|
    20 -------------------4|
    Duration: 20
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        results = []
        for identity, sleep in enumerate([1.0, 0.5, 1.5, 2.0], 1):
            results.append(self.a(identity=identity, sleep=sleep, result=identity))
        for result in results:
            wait(result)


class ParallelSum(object):
    """Similar with ParallelWait but instead of just waiting, compute the sum
    of the results. The wait here is implicit.

    10 ---------1----------|
  R  0                     10
     5 ----2---------------|
    15 --------------3-----|
    20 -------------------4|
    Duration: 20
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        results = []
        for identity, sleep in enumerate([1.0, 0.5, 1.5, 2.0], 1):
            results.append(self.a(identity=identity, sleep=sleep, result=identity))
        return sum(results)


class WaitFirst(object):
    """Parallelize activity calls and wait for the first to finish, ignoring
    the others.
    10 ---------1
  R  5 ----2
    15 --------------3
    20 -------------------4
    Duration: 5
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        x1 = self.a(sleep=1.0, result=1, identity=1)
        x2 = self.a(sleep=0.5, result=2, identity=2)
        x3 = self.a(sleep=1.5, result=3, identity=3)
        x4 = self.a(sleep=2.0, result=4, identity=4)
        return first(x1, x2, x3, x4)


class WaitFirstTwo(object):
    """Parallelize activity calls and wait for the first two activities to
    finish, ignoring the others.
    10 ---------1|
  R  0           3
     5 ----2-----|
    15 --------------3
    20 -------------------4
    Duration: 10
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        x1 = self.a(sleep=1.0, result=1, identity=1)
        x2 = self.a(sleep=0.5, result=2, identity=2)
        x3 = self.a(sleep=1.5, result=3, identity=3)
        x4 = self.a(sleep=2.0, result=4, identity=4)
        f_o = list(finish_order(x1, x2, x3, x4))
        return f_o[0] + f_o[1]
        # Alternatively, if the result is not important, just wait
        # f_o = finish_order(x1, x2, x3, x4)
        # wait(next(f_o))
        # wait(next(f_o))


class Conditional(object):
    """Start an activity conditioned by the result of a previous activity.

  R  0           1
    10 ---------?|
  R 10           ---------2

    Duration: 10, 20
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        x = self.a(result='random', identity=1, sleep=1.0)
        if x > 0.5:
            return self.a(identity='in if', result=2, sleep=1.0)
        return 1


class NaiveMapReduce(object):
    """A naive map reduce implementation.
    This works for non associative and non commutative reduce functions.
    No reduce operation can take place in parallel.

     5 ----1----------|
    15                --------------3|
    15                |              --------------6---------------|
    15                |              |                             -------------10|
    15                |              |                             |              -------------15|
  R 15                |              |                             |              |              -------------21
    15 --------------2|              |                             |              |              |
    10 ---------3--------------------|                             |              |              |
    60 -----------------------------------------------------------4|              |              |
    50 -------------------------------------------------5-------------------------|              |
    20 -------------------6----------------------------------------------------------------------|
    Duration: 105
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        reduce_f = lambda x, y: self.a(sleep=1.5, identity='reduce %s %s' % (x, y), result=x + y)
        map_f = lambda sleep, result: self.a(sleep=sleep, result=result, identity='map %s' % result)
        results = map(map_f, [0.5, 1.5, 1.0, 6.0, 5.0, 2.0], range(1, 7))
        return reduce(reduce_f, results)


class FinishOrderMapReduce(object):
    """A better map reduce implementation.
    The reduction is started as soon as any 2 map operations are completed, and
    continues when the last reduce and a new map operation are completed.
    No reduce operation can take place in parallel.

     5 ----1-----|
    15           --------------4|
    15           |              --------------6|
    15           |              |              -------------12|
    15           |              |              |              -------------17|
  R 15           |              |              |              |              -------------21
    15 ----------|---2----------|              |              |              |
    10 ---------3|                             |              |              |
    60 ----------------------------------------|--------------|---4----------|
    50 ----------------------------------------|--------5-----|
    20 -------------------6--------------------|
    Duration: 85
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        reduce_f = lambda x, y: self.a(sleep=1.5, identity='reduce %s %s' % (x, y), result=x + y)
        map_f = lambda sleep, result: self.a(sleep=sleep, result=result, identity='map %s' % result)
        results = map(map_f, [0.5, 1.5, 1.0, 6.0, 5.0, 2.0], range(1, 7))
        return reduce(reduce_f, finish_order(results))


class ParallelMapReduce(object):
    """A parallel map reduce implementation.
    The reduction is started as soon as any 2 tasks are completed, including
    previous reductions.
    The reduce operations are parallelized.

     5 ----1-----|
    15           --------------4----------|
    15           |                        -------------12|
    15           |                        |              -------------17|
  R 15           |                        |              |              -------------21
    15 ----------|---2-----|              |              |              |
    15           |         --------------8|              |              |
    10 ---------3|         |                             |              |
    60 --------------------|-----------------------------|--------4-----|
    50 --------------------|----------------------------5|
    20 -------------------6|
    Duration: 80
    """
    def __init__(self, a):
        self.a = a

    def __call__(self):
        reduce_f = lambda x, y: self.a(sleep=1.5, identity='reduce %s %s' % (x, y), result=x + y)
        map_f = lambda sleep, result: self.a(sleep=sleep, result=result, identity='map %s' % result)
        results = map(map_f, [0.5, 1.5, 1.0, 6.0, 5.0, 2.0], range(1, 7))
        return parallel_reduce(reduce_f, results)


def main():
    def workflow(workflow_class):
        try:
            return globals()[workflow_class]
        except KeyError:
            raise ValueError('Workflow "%s" not found.' % workflow_class)

    parser = argparse.ArgumentParser(description='Example workflow runner.')
    parser.add_argument('workflow', action='store', type=workflow)
    parser.add_argument('--pure', action='store_true', default=False)
    parser.add_argument('--workflow-workers', action='store', type=int, default=2)
    parser.add_argument('--activity-workers', action='store', type=int, default=8)
    parser.add_argument('--timeit', action='store_true', default=False)
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
        result = lw.run()
    if args.timeit:
        print('Timed at:', time.time() - start)
    print('Result:', result)


if __name__ == '__main__':
    main()
