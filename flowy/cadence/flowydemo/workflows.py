class NoopWorkflow(object):
    """A simple workflows whiout any activities."""
    def __call__(self):
        print 'nothing to do'
        return 'done'


class SimpleWorkflow(object):
    """Receive some input and return the result of an activity."""
    def __init__(self, sum_activity):
        self.sum_activity = sum_activity

    def __call__(self, x, y):
        result = self.sum_activity(x, y)
        print '%s finished with: %s' % (self.__class__.__name__, result)
        return result


class SumAndMulWorkflow(object):
    """Use multiple interdependent activities."""
    def __init__(self, sum_activity, mul_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity

    def __call__(self, x, y, z):
        a = self.sum_activity(x, 1)
        b = self.sum_activity(y, 2)
        c = self.sum_activity(x, 3)
        result = self.mul_activity(self.mul_activity(a, b), c)
        print '%s finished with: %s' % (self.__class__.__name__, result)
        return result


class SumAndMulWorkflow2(object):
    """Same as SumAndMulWorkflow but with dynamic features and map/reduce.

    This is intended to demonstrate that any Python code works because there is
    no static analysis involved.
    """
    def __init__(self, sum_activity, mul_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity

    def __call__(self, x, y, z):
        result = reduce(
            getattr(self, 'mul_activity'),
            map(getattr(self, 'sum_activity'), [x, y, z], [1, 2, 3])
        )
        print '%s finished with: %s' % (self.__class__.__name__, result)
        return result


def SumAndMulWorkflow3(sum_activity, mul_activity):
    """Again, same as SumAndMulWorkflow but with a closure not a class."""
    def workflow(x, y, z):
        result = mul_activity(
            mul_activity(sum_activity(x, 1), sum_activity(y, 2)),
            sum_activity(z, 3)
        )
        print '%s finished with: %s' % ('SumAndMulWorkflow3', result)
        return result
    return workflow


class SumAndMulWorkflow4(object):
    """Like SumAndMulWorkflow but with 1s activity delay.

    This is intended to show the concurrency. With sufficient activity workers
    this workflow finishes in 3 seconds: all sums are computed in parallel,
    then d then e.
    """
    def __init__(self, sum_activity, mul_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity

    def __call__(self, x, y, z):
        a = self.sum_activity(x, 1, sleep=1)
        b = self.sum_activity(y, 2, sleep=1)
        c = self.sum_activity(x, 3, sleep=1)
        d = self.mul_activity(a, b, sleep=1)
        result = self.mul_activity(d, c, sleep=1)
        print '%s finished with: %s' % (self.__class__.__name__, result)
        return result


class ResultConditionalWorkflow(object):
    """Blocks until an activity result is available.

    Whenever a result is dereferenced, it blocks until available.
    In this example, the if conditional converts the result to a boolean.

    When a result is dereferenced, if it's unavailable (i.e. the task -actvity
    or subworkflow- didn't finish yet) the workflow "scan" (current execution)
    stops, and no further task executions are detected.
    """
    def __init__(self, sum_activity, mul_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity

    def __call__(self, x):
        if self.sum_activity(x, x) > 100:
            return self.mul_activity(x, x)
        return 0


class ImplicitErrorPropagationWorkflow(object):
    """Task errors propagate through arguments.

    After err_activity finishes, a holds an error. And, because it's passed to
    sum_actvity, b immediately becomes an error as well. Same for c. Finally,
    the workflow execution fails with whatever error a holds.
    """
    def __init__(self, sum_activity, mul_activity, err_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity
        self.err_activity = err_activity

    def __call__(self):
        a = self.err_activity('test')
        b = self.sum_activity(a, 10)
        c = self.mul_activity(b, 10)
        return c


class ExplicitErrorHandlingWorkflow(object):
    """When a result is dereferenced, it can throw exceptions.

    If the underlying task failed, dereferencing a task can throw exceptions
    which can be handled as usual.
    """
    def __init__(self, err_activity):
        self.err_activity = err_activity

    def __call__(self):
        a = self.err_activity('test')
        import flowy
        try:
            if a > 10:
                # dosomething
                pass
        except flowy.TaskTimedout:  # subclass of TaskError
            return 'timedout'
        except flowy.TaskError:
            return 'fail'
        return 'success'


class ExplicitResultDereferenceWorkflow(object):
    """A task result can be waited (a no-op dereference)."""
    def __init__(self, err_activity):
        self.err_activity = err_activity

    def __call__(self):
        a = self.err_activity('test')
        import flowy
        try:
            flowy.wait(a)
        except flowy.TaskError:
            return 'fail'
        return 'success'


class UnhandledExceptionWorkflow(object):
    """A workflow fails when unhandled exceptions escape.

    This can also happen when dereferencing a result without proper
    error-check.
    """
    def __call__(self):
        raise ValueError('err')


class RestartingWorkflow(object):
    """A workflow can restart itself with different input."""
    def __call__(self, n):
        import flowy
        if n % 2 == 0:
                return flowy.restart(n + 1)
        return n


class Subworkflows(object):
    """A workflow can use both activities and subworkflows.

    A workflow doesn't know anything about what it's dependencies are. For
    example, an activity can be reconfigured to be a subworkflow transparently,
    without changing the wokflow code itself.

    In this case, this workflow is configured as follows:
        * compute_length is SumAndMulWorkflow, configured as a subworkflow.
        * compute_with is the sum activity.
    """
    def __init__(self, compute_length, compute_width):
        self.compute_length = compute_length
        self.compute_width = compute_width

    def __call__(self, x, y, z):
        return self.compute_length(x, y, z) * self.compute_width(x, y)


class WaitForFirstWorkflow(object):
    """A workflow that returns the first result."""
    def __init__(self, sum_activity, mul_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity

    def __call__(self, x, y):
        import flowy
        # Notice: one of the activity will fail because the workflow finishes
        # before the activity sends the result. This is expected.
        return flowy.first(
            self.sum_activity(x, y, sleep=2),
            self.mul_activity(x, y, sleep=1),
            # more calls can go here
        )  # returns the multiplication


class WaitForFirstNWorkflow(object):
    """A workflow that returns the first N results."""
    def __init__(self, sum_activity):
        self.sum_activity = sum_activity

    def __call__(self, x):
        import flowy

        # Returns an iterator of results in finish order
        results = flowy.finish_order(
            self.sum_activity(x, 1, sleep=4),
            self.sum_activity(x, 2, sleep=1),
            self.sum_activity(x, 3, sleep=2),
            self.sum_activity(x, 4, sleep=0),
        )

        # The workflow finishes as soon as any 2 of the 4 tasks finish.
        # For this to be deterministic, there must be at least 4 free activity
        # workers.
        return list(results)[:2]


class ParallelReduceWorkflow(object):
    """Showcase parallel_reduce.

    parallel_reduce is a replacement of the regular reduce for associative and
    commutative reduce functions. Instead of reducing in order, the reduction
    starts as soon as results are available (at any depth level).

    This code is an optimization example of SumAndMulWorkflow: if the first and
    the 3rd map finishes before the 2nd map, the reduction starts.
    """
    def __init__(self, sum_activity, mul_activity):
        self.sum_activity = sum_activity
        self.mul_activity = mul_activity

    def __call__(self, x, y, z):
        import flowy
        return flowy.parallel_reduce(
            self.mul_activity,
            map(self.sum_activity, [x, y, z], [1, 2, 3])
        )
