from flowy.proxy import SWFActivityProxy as ActivityProxy
from flowy.scanner import swf_activity as activity
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFWorkflow as Workflow
from flowy.tests.integration.dependency import Double

# make Double available for the scanner
Double = activity(1)(Double)


@workflow(1)
class Sequence(Workflow):
    """ A sequential set of operations. """

    double = ActivityProxy('Double', 1, task_list='example_list2',
                           heartbeat=10, schedule_to_close=20,
                           schedule_to_start=10, start_to_close=15)

    def run(self, start=1):
        r = self.double(start)
        while r.result() < 100:
            r = self.double(r)
        return r


runs = [
    {
        'name': 'Sequence',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
