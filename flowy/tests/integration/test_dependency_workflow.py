from mock import patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
from WorkflowTestCase import WorkflowTestCase, load_json_responses


@workflow_config('LoopyWorkflow', 2, 'a_list', 60, 60)
class LoopyWorkflow(Workflow):
    """
    Executes two activites. One of them is passed to the other one as argument.

    """
    loop = ActivityProxy(
        name='RangeActivity',
        version=2,
        task_list='a_list',
    )
    op = ActivityProxy(
        name='OperationActivity',
        version=2,
        task_list='a_list'
    )

    def run(self, remote):
        r = remote.loop()
        res = remote.op(r)
        res.result()


class LoopyWorflowTest(WorkflowTestCase):

    @load_json_responses("multiple/dependent_activity_worklow.txt")
    def test_workflow(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        LoopyWorkflowID = my_config.workflow_starter('LoopyWorkflowID', 1)
        print 'Starting: ', LoopyWorkflowID()

        # Run one decision task
        my_config.scan()

        for _ in range(3):
            my_config._client.dispatch_next_decision(task_list='a_list')

        self.assertCompletedWorkflow(requests)
