from flowy.proxy import SWFActivityProxy as ActivityProxy
from flowy.scanner import swf_activity as activity
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFActivity as Activity
from flowy.task import SWFWorkflow as Workflow


@workflow(1)
class Dependency(Workflow):

    identity = ActivityProxy('Identity', 1, task_list='example_list2',
                             heartbeat=10, schedule_to_close=20,
                             schedule_to_start=10, start_to_close=15)
    double = ActivityProxy('Double', 1, task_list='example_list2',
                           heartbeat=10, schedule_to_close=20,
                           schedule_to_start=10, start_to_close=15)
    sum = ActivityProxy('Sum', 1, task_list='example_list2',
                        heartbeat=10, schedule_to_close=20,
                        schedule_to_start=10, start_to_close=15)

    def run(self, start_value):
        a = self.identity(start_value)
        b = self.double(a)
        c = self.double(b)
        d = self.identity(start_value)
        e = self.double(d)
        return self.sum(a, b, c, d, e)


@activity(1)
class Identity(Activity):
    def run(self, value):
        return value


@activity(1)
class Double(Activity):
    def run(self, value):
        return value + value


@activity(1)
class Sum(Activity):
    def run(self, *args):
        return sum(args)


runs = [
    {
        'name': 'Dependency',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
        'args': [10],
    },
]
