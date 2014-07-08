from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFWorkflow as Workflow


@workflow(2)
class Simple(Workflow):
    """ Does nothing, just returns the argument it receives. """
    def run(self, value='hello'):
        return value


runs = [
    {
        'name': 'Simple',
        'version': 2,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
    {
        'name': 'Simple',
        'version': 2,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
        'args': ['world'],
    }
]
