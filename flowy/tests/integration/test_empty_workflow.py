from mock import patch
from flowy import Workflow
from flowy import make_config, workflow_config
from boto.swf.layer1 import Layer1
from WorkflowTestCase import WorkflowTestCase, load_json_responses


@workflow_config('BlankWorkflow', 3, 'a_list', 60, 60)
class BlankWorkflow(Workflow):
    """
    Does nothing

    """

    def run(self, remote):
        print("smt")
        return True


class BlankWorkflowTest(WorkflowTestCase):

    @load_json_responses("blank/mocks_output.txt")
    def test_workflow_registration(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        BlankWorkflowId = my_config.workflow_starter('BlankWorkflow', 3)
        print 'Starting: ', BlankWorkflowId()

        my_config.scan()
        my_config._client.dispatch_next_decision('a_list')

        self.assertCompletedWorkflow(requests)
