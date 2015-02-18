from flowy import restart


class NoTask(object):
    def __call__(self, n):
        return n


def Closure():
    def run(n):
        return n
    return run


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
        a.wait()
        b = self.task(a)
        return b


class Restart(object):
    def __init__(self, task):
        self.task = task

    def __call__(self, r=True):
        a = self.task()
        return restart(a, 2)
