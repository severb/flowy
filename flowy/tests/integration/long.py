from flowy.proxy import SWFActivityProxy as ActivityProxy
from flowy.scanner import swf_activity as activity
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFWorkflow as Workflow
from flowy.tests.integration.dependency import Identity

# make Double available for the scanner
Identity = activity(1)(Identity)


@workflow(1)
class Long(Workflow):

    identity = ActivityProxy('Identity', 1, task_list='example_list2',
                             heartbeat=10, schedule_to_close=60,
                             schedule_to_start=40, start_to_close=20)

    def run(self):
        numbers = [self.identity(x) for x in range(100)]
        return sum(n.result() for n in numbers)


runs = [
    {
        'name': 'Long',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 40,
    },
]
