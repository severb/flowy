from flowy.proxy import SWFActivityProxy as ActivityProxy
from flowy.scanner import swf_activity as activity
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFActivity as Activity
from flowy.task import SWFWorkflow as Workflow
from flowy.tests.integration.dependency import Double

# make Double available for the scanner
Double = activity(1)(Double)


@activity(1)
class Sum(Activity):
    def run(self, *n):
        return sum(n)


@workflow(1)
class MapReduce(Workflow):
    """ A toy map reduce example. """

    double = ActivityProxy('Double', 1, task_list='example_list2',
                           heartbeat=10, schedule_to_close=20,
                           schedule_to_start=10, start_to_close=15)
    sum = ActivityProxy('Sum', 1, task_list='example_list2',
                        heartbeat=10, schedule_to_close=20,
                        schedule_to_start=10, start_to_close=15)

    def run(self, n=10):
        doubles = map(self.double, range(n))
        return self.sum(*doubles)

runs = [
    {
        'name': 'MapReduce',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
