from mock import patch
from flowy import Workflow, ActivityProxy
from flowy import make_config, workflow_config
from boto.swf.layer1 import Layer1
from WorkflowTestCase import WorkflowTestCase, load_json_responses

@workflow_config('DelayWorkflow', 1, 'constant_list', 60, 60)
class DelayWorkflow(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SimpleActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        print("what?")
        with remote.options(delay=5):
            r = remote.div()
            print(r.result())


class DelayedWorkflowTest(WorkflowTestCase):

    @load_json_responses("delayed/delayed_workflow.txt")
    def test_workflow(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        DelayedWorkflowID = my_config.workflow_starter('DelayWorkflow', 1)
        print 'Starting: ', DelayedWorkflowID()

        # Run one decision task
        my_config.scan()

        for _ in range(3):
            my_config._client.dispatch_next_decision(task_list='constant_list')

        self.assertCompletedWorkflow(requests)
