from flowy.swf.scanner import workflow
from flowy.swf.task import ActivityProxy, WorkflowProxy
from flowy.task import TaskError, Workflow


@workflow('SimpleReturnExample', 77, 'example_list')
class SimpleReturn(Workflow):
    """ Does nothing, just returns the argument it receives. """
    def run(self, value='hello'):
        return value


@workflow('ActivityReturnExample', 77, 'example_list')
class ActivityReturn(Workflow):
    """ Returns the value of the activity. """

    identity = ActivityProxy('Identity', 77)

    def run(self):
        return self.identity('activity return')


@workflow('SimpleDependencyExample', 77, 'example_list')
class SimpleDependency(Workflow):
    """ Some tasks that depend on other task results. """

    identity = ActivityProxy('Identity', 77)
    double = ActivityProxy('Double', 77)
    sum = ActivityProxy('Sum', 77)

    def run(self):
        a = self.identity(10)
        b = self.double(a)
        c = self.double(b)
        d = self.identity(100)
        return self.sum(a, b, c, d).result()


@workflow('SequenceExample', 77, 'example_list')
class Sequence(Workflow):
    """ A sequential set of operations. """

    double = ActivityProxy('Double', 77)

    def run(self, n=5):
        n = int(n)  # when starting a workflow from cmdline this is a string
        double = self.double(n)
        while double.result() < 100:
            double = self.double(double)
        return double.result() - 100


@workflow('MapReduceExample', 77, 'example_list')
class MapReduce(Workflow):
    """ A toy map reduce example. """

    square = ActivityProxy('Square', 77)
    sum = ActivityProxy('Sum', 77)

    def run(self, n=5):
        n = int(n)
        squares = map(self.square, range(n))
        return self.sum(*squares)


@workflow('DelayActivityExample', 77, 'example_list')
class Delay(Workflow):
    """ Call tasks with different delays. """

    identity = ActivityProxy('Identity', 77)
    delayed_identity = ActivityProxy('Identity', 77, delay=5)

    def run(self):
        self.identity('no delay')
        self.delayed_identity('5 delay')
        with self.options(delay=10):
            self.identity('10 dealy')


@workflow('UnhandledErrorExample', 77, 'example_list')
class UnhandledError(Workflow):
    """ When a task has an error the workflow will immediately fail. """

    error = ActivityProxy('Error', 77)

    def run(self):
        self.error('I errd!')


@workflow('HandledErrorExample', 77, 'example_list')
class HandledError(Workflow):
    """ A failed task can be intercepted and handled correctly. """

    error = ActivityProxy('Error', 77)
    handled_error = ActivityProxy('Error', 77, error_handling=True)

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


@workflow('ErrorChainingExample', 77, 'example_list')
class ErrorChaining(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    enabled will generate a new fail result.

    """
    error = ActivityProxy('Error', 77)
    identity = ActivityProxy('Identity', 77)

    def run(self):
        with self.options(error_handling=True):
            a = self.error('err!')
            b = self.identity(a)
            c = self.identity(b)
        try:
            c.result()
        except TaskError:
            pass


@workflow('ErrorResultPassedExample', 77, 'example_list')
class ErrorResultPassed(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    disabled will immediately fail the entire workflow.

    """
    error = ActivityProxy('Error', 77)
    identity = ActivityProxy('Identity', 77)

    def run(self):
        with self.options(error_handling=True):
            a = self.error('err!')
        return self.identity(a).result()


@workflow('ErrorInWorkflowExample', 77, 'example_list')
class ErrorInWorkflow(Workflow):
    """ An unhandled exception in the run method will stop the workflow. """

    def run(self):
        raise ValueError('stop')


@workflow('TimeoutExample', 77, 'example_list')
class Timeout(Workflow):
    """ A task that timesout will stop the workflow if it's unhandled. """

    timeout = ActivityProxy('Timeout', 77)

    def run(self):
        self.timeout()


@workflow('SubworkflowExample', 77, 'example_list')
class SubworkflowExample(Workflow):
    """ Start a subworkflow. """

    subwf = WorkflowProxy('SimpleReturnExample', 77)

    def run(self):
        return self.subwf()
