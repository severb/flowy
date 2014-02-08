from flowy.swf.scanner import workflow
from flowy.swf.task import ActivityProxy
from flowy.task import Workflow, TaskError, TaskTimedout


@workflow('SimpleReturnExample', 1, 'example_list')
class SimpleReturn(Workflow):
    """ Does nothing, just returns the argument it receives. """
    def run(self, value):
        return value


@workflow('ActivityReturnExample', 1, 'example_list')
class ActivityReturn(Workflow):
    """ Returns the value of the activity. """

    identitiy = ActivityProxy('Identity', 1)

    def run(self):
        return self.identity_activity('activity return')


@workflow('SimpleDependencyExample', 1, 'example_list')
class SimpleDependency(Workflow):
    """ Some tasks that depend on other task results. """

    identitiy = ActivityProxy('Identity', 1)
    double = ActivityProxy('Double', 1)
    sum = ActivityProxy('Sum', 1)

    def run(self):
        a = self.identity_activity(10)
        b = self.double(a)
        c = self.double(b)
        d = self.identity_activity(100)
        return self.sum(a, b, c, d).result()


@workflow('SequenceExample', 1, 'example_list')
class Sequence(Workflow):
    """ A sequential set of operations. """

    double = ActivityProxy('Double', 1)

    def run(self, n):
        double = self.double(n)
        while double.result() < 100:
            double = self.double(double)
        return double.result() - 100


@workflow('MapReduceExample', 1, 'example_list')
class MapReduce(Workflow):
    """ A toy map reduce example. """

    square = ActivityProxy('Square', 1)
    sum = ActivityProxy('Square', 1)

    def run(self, n):
        squares = (s.result() for s in map(self.square, range(n)))
        return self.sum(*squares).result()


@workflow('DelayActivityExample', 1, 'example_list')
class Delay(Workflow):
    """ Call tasks with different delays. """

    identity = ActivityProxy('Identity', 1)
    delayed_identity = ActivityProxy('Identity', 1, delay=5)

    def run(self):
        self.identity('no delay')
        self.delayed_identity('5 delay')
        with self.options(delay=10):
            self.identitiy('10 dealy')


@workflow('UnhandledErrorExample', 1, 'example_list')
class UnhandledError(Workflow):
    """ When a task has an error the workflow will immediately fail. """

    error = ActivityProxy('Error', 1)

    def run(self):
        self.error('I errd!')


@workflow('HandledErrorExample', 1, 'example_list')
class HandledError(Workflow):
    """ A failed task can be intercepted and handled correctly. """

    error = ActivityProxy('Error', 1)
    handled_error = ActivityProxy('Error', 1, error_handling=True)

    def run(self):
        with self.options(error_handling=True):
            a = self.error('catch me')
        b = self.handled_error('catch me too')
        try:
            a.result()
        except TaskError:
            pass
        try:
            b.result()
        except TaskError:
            pass


@workflow('ErrorChainingExample', 1, 'example_list')
class ErrorChaining(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    enabled will generate a new fail result.

    """
    error = ActivityProxy('Error', 1)
    identity = ActivityProxy('Identity', 1)

    def run(self):
        with self.options(error_handling=True):
            a = self.error('err!')
            b = self.identity(a)
            c = self.identity(b)
        try:
            c.result()
        except TaskError:
            pass


@workflow('ErrorResultPassedExample', 1, 'example_list')
class ErrorResultPassed(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    disabled will immediately fail the entire workflow.

    """
    error = ActivityProxy('Error', 1)
    identity = ActivityProxy('Identity', 1)

    def run(self):
        with self.options(error_handling=True):
            a = self.error('err!')
        return self.identitiy(a)


@workflow('ErrorInWorkflowExample', 1, 'example_list')
class ErrorInWorkflow(Workflow):
    """ An unhandled exception in the run method will stop the workflow. """

    def run(self):
        raise ValueError('stop')


@workflow('RetryExample', 1, 'example_list')
class Retry(Workflow):
    """ A task that times out will be retried a few times. """

    timeout = ActivityProxy('Timeout', 1)
    retry_timeout = ActivityProxy('Timeout', 1, retry=10)

    def run(self):
        with self.options(retry=10):
            self.timeout()
        self.retry_timeout()
        with self.options(error_handling=True):
            a = self.timeout()
        try:
            a.result()
        except TaskTimedout:
            pass


@workflow('TimeoutExample', 1, 'example_list')
class Timeout(Workflow):
    """ A task that timesout will stop the workflow if it's unhandled. """

    timeout = ActivityProxy('Timeout', 1)

    def run(self):
        self.timeout()
