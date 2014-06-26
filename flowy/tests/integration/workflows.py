from flowy.exception import TaskError
from flowy.proxy import SWFActivityProxy, SWFWorkflowProxy
from flowy.scanner import swf_workflow
from flowy.task import SWFWorkflow


Workflow = SWFWorkflow
ActivityProxy, WorkflowProxy = SWFActivityProxy, SWFWorkflowProxy
workflow = swf_workflow


identity_activity = ActivityProxy(
    'Identity', 7, task_list='example_list', heartbeat=10,
    schedule_to_close=10, schedule_to_start=20, start_to_close=40)
double_activity = ActivityProxy(
    'Double', 7, task_list='example_list', heartbeat=10,
    schedule_to_close=10, schedule_to_start=20, start_to_close=40)
sum_activity = ActivityProxy(
    'Sum', 7, task_list='example_list', heartbeat=10,
    schedule_to_close=10, schedule_to_start=20, start_to_close=40)
square_activity = ActivityProxy(
    'Square', 7, task_list='example_list', heartbeat=10,
    schedule_to_close=10, schedule_to_start=20, start_to_close=40)
error_activity = ActivityProxy(
    'Error', 7, task_list='example_list', heartbeat=10,
    schedule_to_close=10, schedule_to_start=20, start_to_close=40)
heartbeat_activity = ActivityProxy(
    'Heartbeat', 7, task_list='example_list', heartbeat=10,
    schedule_to_close=10, schedule_to_start=20, start_to_close=40)


@workflow(7, name='SimpleReturnExample')
class Simple(Workflow):
    """ Does nothing, just returns the argument it receives. """
    def run(self, value='hello'):
        return value


@workflow(7, decision_duration=10)
class ActivityReturnExample(Workflow):
    """ Returns the value of the activity. """

    identity = identity_activity

    def run(self):
        return self.identity('activity return')


@workflow(7, workflow_duration=30)
class SimpleDependencyExample(Workflow):
    """ Some tasks that depend on other task results. """

    identity = identity_activity
    double = double_activity
    sum = sum_activity

    def run(self):
        a = self.identity(10)
        b = self.double(a)
        c = self.double(b)
        d = self.identity(100)
        return self.sum(a, b, c, d).result()


@workflow(7, task_list='example_list')
class SequenceExample(Workflow):
    """ A sequential set of operations. """

    double = double_activity

    def run(self, n=5):
        n = int(n)  # when starting a workflow from cmdline this is a string
        double = self.double(n)
        while double.result() < 100:
            double = self.double(double)
        return double.result() - 100


@workflow(7)
class MapReduceExample(Workflow):
    """ A toy map reduce example. """

    square = square_activity
    sum = sum_activity

    def run(self, n=5):
        n = int(n)
        squares = map(self.square, range(n))
        return self.sum(*squares)


@workflow(7)
class DelayActivityExample(Workflow):
    """ Call tasks with different delays. """

    identity = identity_activity
    delayed_identity = ActivityProxy(
        'Identity', 7, task_list='example_list', schedule_to_close=10,
        schedule_to_start=20, start_to_close=40, delay=5)

    def run(self):
        self.identity('no delay')
        self.delayed_identity('5 delay')
        with self.identity.options(delay=10):
            self.identity('10 dealy')


@workflow(7)
class UnhandledErrorExample(Workflow):
    """ When a task has an error the workflow will immediately fail. """

    error = error_activity

    def run(self):
        self.error('I errd!')


@workflow(7)
class HandledErrorExample(Workflow):
    """ A failed task can be intercepted and handled correctly. """

    error = error_activity
    handled_error = ActivityProxy(
        'Error', 7, task_list='example_list', schedule_to_close=10,
        schedule_to_start=20, start_to_close=40, error_handling=True)

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


@workflow(7)
class ErrorChainingExample(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    enabled will generate a new fail result.

    """
    error = error_activity
    identity = identity_activity

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


@workflow(7)
class ErrorResultPassedExample(Workflow):
    """
    Passing the result of a failed task into another task with error handling
    disabled will immediately fail the entire workflow.

    """
    error = error_activity
    identity = identity_activity

    def run(self):
        with self.error.options(error_handling=True):
            a = self.error('err!')
        return self.identity(a).result()


@workflow(7)
class ErrorInWorkflowExample(Workflow):
    """ An unhandled exception in the run method will stop the workflow. """

    def run(self):
        raise ValueError('stop')


@workflow(7)
class TimeoutExample(Workflow):
    """ A task that timesout will stop the workflow if it's unhandled. """

    timeout = ActivityProxy(
        'Timeout', 7, task_list='example_list', heartbeat=1, # force timeout
        schedule_to_close=10, schedule_to_start=20, start_to_close=40)

    def run(self):
        self.timeout()


@workflow(7)
class SubworkflowExample(Workflow):
    """ Start a subworkflow. """

    subwf = WorkflowProxy(
        'SimpleReturnExample', 7, task_list='example_list',
        decision_duration=10, workflow_duration=20)

    def run(self):
        return self.subwf()


@workflow(7)
class HBExample(Workflow):

    h = heartbeat_activity

    def run(self):
        return self.h()


@workflow(7)
class RestartExample(Workflow):
    def run(self, restart=0):
        if restart == 0:
            self.restart(1)
        if restart == 1:
            with self.options(decision_duration=100, workflow_duration=200,
                              tags=['a', 'b']):
                self.restart(2)
