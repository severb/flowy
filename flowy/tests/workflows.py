from flowy import restart


class QuickReturn(object):
    def __call__(self, n):
        return n


def Closure():
    def run(self, n):
        return n
    return run


class Arguments(object):
    def __call__(self, a, b, c=1, d=2):
        return a, b, c, d


class Dependency(object):
    def __init__(self, inc):
        self.inc = inc

    def __call__(self, n):
        accumulator = self.inc(0)
        for _ in range(n):
            accumulator = self.inc(accumulator)
        return accumulator.result()


class Parallel(object):
    def __init__(self, inc):
        self.inc = inc

    def __call__(self, n):
        results = map(self.inc, range(n))
        return [r.result() for r in results]


class UhnadledException(object):
    def __call__(self):
        raise RuntimeError('err!')


class ActivityException(object):
    def __init__(self, err):
        self.err = err

    def __call__(self):
        return self.err().result()


class ActivityExceptionPropagation(object):
    def __init__(self, err, inc):
        self.err = err
        self.inc = inc

    def __call__(self):
        a = self.err()
        b = self.inc(a)
        c = self.inc(b)
        return c.result()


class Recurse(object):
    def __init__(self, myself):
        self.myself = myself

    def __call__(self, recurse=True):
        if recurse:
            return self.myself(recurse=False).result()
        else:
            return 1


class Restart(object):
    def __call__(self, r=True):
        if r:
            return restart(r=False)
        else:
            return 1
