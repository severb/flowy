from flowy import finish_order
from flowy import first
from flowy import parallel_reduce
from flowy import restart
from flowy import SWFWorkflow
from flowy import wait


w_no_name = SWFWorkflow(version=1)
w_named = SWFWorkflow(name='Named', version=1)


@w_no_name
class NoTask(object):
    def __call__(self, n):
        return n


@w_no_name
def Closure():
    def run(n):
        return n
    return run


@w_named
class Arguments(object):
    def __call__(self, a, b, c=1, d=2):
        return a, b, c, d


class Dependency(object):
    def __init__(self, task):
        self.task = task

    def __call__(self, n):
        accumulator = self.task(0)
        for _ in range(n):
            accumulator = self.task(accumulator)
        return accumulator


class Parallel(object):
    def __init__(self, task):
        self.task = task

    def __call__(self, n):
        return list(map(self.task, range(n)))


class UnhandledException(object):
    def __call__(self):
        raise RuntimeError('err!')


class SingleTask(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        return self.task()


class WaitTask(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        a = self.task()
        wait(a)
        b = self.task(a)
        return b


class Restart(object):
    def __init__(self, task):
        self.task = task

    def __call__(self, r=True):
        a = self.task()
        return restart(a, 2)


class PreRun(object):
    def __init__(self, task):
        self.a = task()
        self.b = task(self.a)

    def __call__(self):
        return self.b


class PreRunError(object):
    def __init__(self):
        raise RuntimeError('err!')

    def __call__(self):
        pass


class PreRunWait(object):
    def __init__(self, task):
        a = task()
        wait(a)
        self.b = task(a)

    def __call__(self):
        return self.b


class DoubleDep(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        a = self.task()
        b = self.task()
        c = self.task(a=a, b=b, c=3, d=4)
        return c


class First(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        a = self.task()
        b = self.task()
        return first(a, b)


class First2(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        a = finish_order([self.task() for _ in range(4)])
        return next(a), next(a)


class ParallelReduce(object):
    def __init__(self, task, red):
        self.task = task
        self.red = red

    def __call__(self):
        a = self.task()
        b = self.task()
        c = self.task()
        return parallel_reduce(self.red, (a, b, c))


class ParallelReduceCombined(object):
    def __init__(self, task, red):
        self.task = task
        self.red = red

    def __call__(self):
        a = self.task()
        return parallel_reduce(self.red, (a, 'a', 'b', 'c'))
