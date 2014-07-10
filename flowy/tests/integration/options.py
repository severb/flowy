from flowy.proxy import SWFActivityProxy as ActivityProxy
from flowy.proxy import SWFWorkflowProxy as WorkflowProxy
from flowy.scanner import swf_activity as activity
from flowy.scanner import swf_workflow as workflow
from flowy.task import SWFWorkflow as Workflow
from flowy.tests.integration.dependency import Identity

# make Identity available for the scanner
Identity = activity(1)(Identity)


@workflow(1)
class IdentityW(Workflow):
    identity1 = ActivityProxy('Identity', 1, task_list='example_list2',
                              heartbeat=100, schedule_to_close=200,
                              schedule_to_start=300, start_to_close=400)
    identity2 = ActivityProxy('Identity', 1, task_list='example_list1',
                              heartbeat=10, schedule_to_close=20,
                              schedule_to_start=10, start_to_close=15)

    def run(self):
        with self.identity1.options():
            i1 = self.identity1(100)
        with self.identity2.options(task_list='example_list2', heartbeat=100,
                                    schedule_to_close=200, start_to_close=400,
                                    schedule_to_start=300):
            i2 = self.identity2(100)
        return i1.result() + i2.result()


@workflow(1)
class OptionsTest(Workflow):
    identity1 = WorkflowProxy('IdentityW', 1, task_list='example_list2',
                              decision_duration=100, workflow_duration=200)
    identity2 = WorkflowProxy('IdentityW', 1, task_list='example_list1',
                              decision_duration=10, workflow_duration=20)

    def run(self):
        with self.identity1.options():
            i1 = self.identity1()
        with self.identity2.options(task_list='example_list2',
                                    decision_duration=100, retry=4, delay=1,
                                    workflow_duration=200,
                                    error_handling=True):
            i2 = self.identity2()
        return i1.result() + i2.result()


runs = [
    {
        'name': 'OptionsTest',
        'version': 1,
        'task_list': 'example_list2',
        'workflow_duration': 100,
        'decision_duration': 10,
    },
]
