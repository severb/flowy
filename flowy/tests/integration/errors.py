import os
import time

from flowy.proxy import SWFActivityProxy as ActivityProxy
from flowy.scanner import swf_activity as activity
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFActivity as Activity
from flowy.task import SWFWorkflow as Workflow
from flowy.tests.integration.dependency import Identity

# make Identity available for the scanner
Identity = activity(1)(Identity)


@workflow(1)
class WorkflowFailure(Workflow):
    error = ActivityProxy('Error', 1, task_list='example_list2',
                          heartbeat=5, schedule_to_close=20,
                          schedule_to_start=10, start_to_close=15)

    def run(self):
        return self.error()


@workflow(1)
class ErrorChaining(Workflow):
    identity = ActivityProxy('Identity', 1, task_list='example_list2',
                             heartbeat=10, schedule_to_close=20,
                             schedule_to_start=10, start_to_close=15,
                             error_handling=True)
    error = ActivityProxy('Error', 1, task_list='example_list2',
                          heartbeat=5, schedule_to_close=20,
                          schedule_to_start=10, start_to_close=15,
                          error_handling=True)

    def run(self):
        e = self.error()
        return self.identity(e)


@workflow(1)
class ErrorShortcircuit(Workflow):
    identity = ActivityProxy('Identity', 1, task_list='example_list2',
                             heartbeat=10, schedule_to_close=20,
                             schedule_to_start=10, start_to_close=15)
    error = ActivityProxy('Error', 1, task_list='example_list2',
                          heartbeat=5, schedule_to_close=20,
                          schedule_to_start=10, start_to_close=15,
                          error_handling=True)

    def run(self):
        e = self.error()
        return self.identity(e)


@workflow(1)
class TimeoutFailure(Workflow):
    error = ActivityProxy('Error', 1, task_list='example_list2',
                          heartbeat=1, schedule_to_close=20,
                          schedule_to_start=10, start_to_close=15, retry=0)

    def run(self):
        return self.error(2)


@workflow(1)
class TimeoutChaining(Workflow):
    identity = ActivityProxy('Identity', 1, task_list='example_list2',
                             heartbeat=10, schedule_to_close=20,
                             schedule_to_start=10, start_to_close=15,
                             error_handling=True)
    error = ActivityProxy('Error', 1, task_list='example_list2',
                          heartbeat=1, schedule_to_close=20,
                          schedule_to_start=10, start_to_close=15,
                          error_handling=True, retry=0)

    def run(self):
        t = self.error(2)
        return self.identity(t)


@workflow(1)
class WorkflowError(Workflow):
    def run(self):
        raise ValueError('err!')


@activity(1)
class Error(Activity):
    def run(self, delay=0):
        if delay and not os.environ.get('TESTING'):
            time.sleep(delay)
        raise ValueError('err!')


runs = [
    {
        'name': 'WorkflowFailure',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'ErrorChaining',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'ErrorShortcircuit',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'TimeoutFailure',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'TimeoutChaining',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'WorkflowError',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
