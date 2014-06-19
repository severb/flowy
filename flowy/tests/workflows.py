from flowy.exception import TaskError
from flowy.proxy import SWFActivityProxy, SWFWorkflowProxy
from flowy.scanner import swf_workflow
from flowy.task import SWFWorkflow


Workflow = SWFWorkflow
ActivityProxy, WorkflowProxy = SWFActivityProxy, SWFWorkflowProxy
workflow = swf_workflow


@workflow(77, 'example_list', name='SimpleReturnExample')
class Simple(Workflow):
    """ Does nothing, just returns the argument it receives. """
    def run(self, value='hello'):
        return value


@workflow(77, 'example_list')
class ActivityReturnExample(Workflow):
    """ Returns the value of the activity. """

    identity = ActivityProxy('Identity', 77)

    def run(self):
        return self.identity('activity return')


@workflow(77, 'example_list')
class SimpleDependencyExample(Workflow):
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


@workflow(77, 'example_list')
class SequenceExample(Workflow):
    """ A sequential set of operations. """

    double = ActivityProxy('Double', 77)

    def run(self, n=5):
        n = int(n)  # when starting a workflow from cmdline this is a string
        double = self.double(n)
        while double.result() < 100:
            double = self.double(double)
        return double.result() - 100


@workflow(77, 'example_list')
class MapReduceExample(Workflow):
    """ A toy map reduce example. """

    square = ActivityProxy('Square', 77)
    sum = ActivityProxy('Sum', 77)

    def run(self, n=5):
        n = int(n)
        squares = map(self.square, range(n))
        return self.sum(*squares)


@workflow(77, 'example_list')
class DelayActivityExample(Workflow):
    """ Call tasks with different delays. """

    identity = ActivityProxy('Identity', 77)
    delayed_identity = ActivityProxy('Identity', 77, delay=5)

    def run(self):
        self.identity('no delay')
        self.delayed_identity('5 delay')
        with self.identity.options(delay=10):
            self.identity('10 dealy')


@workflow(77, 'example_list')
class UnhandledErrorExample(Workflow):
    """ When a task has an error the workflow will immediately fail. """

    error = ActivityProxy('Error', 77)

    def run(self):
        self.error('I errd!')


@workflow(77, 'example_list')
class HandledErrorExample(Workflow):
    """ A failed task can be intercepted and handled correctly. """

    error = ActivityProxy('Error', 77)
    handled_error = ActivityProxy('Error', 77, error_handling=True)

    def run(self):
        with self.error.options(error_handling=True):
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


@workflow(77, 'example_list')
class ErrorChainingExample(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    enabled will generate a new fail result.

    """
    error = ActivityProxy('Error', 77)
    identity = ActivityProxy('Identity', 77)

    def run(self):
        with self.error.options(error_handling=True):
            with self.identity.options(error_handling=True):
                a = self.error('err!')
                b = self.identity(a)
                c = self.identity(b)
        try:
            c.result()
        except TaskError:
            pass


@workflow(77, 'example_list')
class ErrorResultPassedExample(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    disabled will immediately fail the entire workflow.

    """
    error = ActivityProxy('Error', 77)
    identity = ActivityProxy('Identity', 77)

    def run(self):
        with self.error.options(error_handling=True):
            a = self.error('err!')
        return self.identity(a).result()


@workflow(77, 'example_list')
class ErrorInWorkflowExample(Workflow):
    """ An unhandled exception in the run method will stop the workflow. """

    def run(self):
        raise ValueError('stop')


@workflow(77, 'example_list')
class TimeoutExample(Workflow):
    """ A task that timesout will stop the workflow if it's unhandled. """

    timeout = ActivityProxy('Timeout', 77)

    def run(self):
        self.timeout()


@workflow(77, 'example_list')
class SubworkflowExample(Workflow):
    """ Start a subworkflow. """

    subwf = WorkflowProxy('SimpleReturnExample', 77)

    def run(self):
        return self.subwf()


@workflow(77, 'example_list')
class RestartWorkflowExample(Workflow):

    def run(self, restarted=False):
        if not restarted:
            with self.options(decision_duration=10, tags=['a', 'b']):
                self.restart(restarted=True)


@workflow(77, 'example_list')
class FailFastExample(Workflow):

    def run(self):
        self.fail('fail fast')
