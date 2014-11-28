from flowy import swf_workflow as workflow
from flowy import SWFActivityProxy as ActivityProxy
from flowy import SWFWorkflow as Workflow
from flowy import SWFWorkflowProxy as WorkflowProxy


@workflow(1)
class ActivityNotFound(Workflow):

    not_found = ActivityProxy('NotFound', 1)

    def run(self):
        return self.not_found()


@workflow(1)
class SubworkflowNotFound(Workflow):

    not_found = WorkflowProxy('NotFound', 1)

    def run(self):
        return self.not_found()


runs = [
    {
        'name': 'ActivityNotFound',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'SubworkflowNotFound',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
