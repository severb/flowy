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


class SingleActivity(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        return self.task()


class ThreeActivities(object):
    def __init__(self, task):
        self.task = task

    def __call__(self):
        a = self.task()
        b = self.task(a)
        return self.task(b)


class Recurse(object):
    def __init__(self, myself):
        self.myself = myself

    def __call__(self, recurse=True):
        if recurse:
            return self.myself(recurse=False)
        else:
            return 1


class Restart(object):
    def __call__(self, r=True):
        if r:
            return restart(r=False)
        else:
            return 1
