from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFWorkflow as Workflow


@workflow(1)
class Restart(Workflow):
    def run(self, restart=0):
        if restart == 0:
            self.restart(1)
        if restart == 1:
            with self.options(decision_duration=100, workflow_duration=200,
                              tags=['a', 'b']):
                self.restart(2)

runs = [
    {
        'name': 'Restart',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
