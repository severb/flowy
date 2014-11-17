from flowy import SWFActivityProxy as ActivityProxy
from flowy import swf_activity as activity
from flowy import swf_workflow as workflow
from flowy import SWFActivity as Activity
from flowy import SWFWorkflow as Workflow


@workflow(1)
class HB(Workflow):

    hb = ActivityProxy('Heartbeat', 1, task_list='example_list2',
                       heartbeat=2, schedule_to_close=20,
                       schedule_to_start=10, start_to_close=15)

    def run(self):
        return self.hb()


@activity(1)
class Heartbeat(Activity):
    def run(self):
        self.heartbeat()
        self.heartbeat()


runs = [
    {
        'name': 'HB',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
