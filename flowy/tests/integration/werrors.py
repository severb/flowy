import os
import time

from flowy.proxy import SWFWorkflowProxy as WorkflowProxy
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFWorkflow as Workflow


@workflow(2)
class WTimeoutFailure(Workflow):
    error = WorkflowProxy('WError', 2, retry=0)

    def run(self):
        return self.error(2)


@workflow(2)
class WFailure(Workflow):
    error = WorkflowProxy('WError', 2, retry=0)

    def run(self):
        return self.error()


@workflow(2, task_list='example_list2', decision_duration=10,
          workflow_duration=1)
class WError(Workflow):
    def run(self, delay=0):
        if not os.environ.get('TESTING'):
            time.sleep(delay)
        raise ValueError('err!')


runs = [
    {
        'name': 'WFailure',
        'version': 2,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 60,
    },
    {
        'name': 'WTimeoutFailure',
        'version': 2,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 60,
    },
]
