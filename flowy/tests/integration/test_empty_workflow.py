import unittest
from mock import create_autospec, MagicMock, patch
from flowy import Workflow
from flowy import make_config, workflow_config
from boto.swf.layer1 import Layer1
import json


@workflow_config('BlankWorkflow', 1, 'a_list', 60, 60)
class BlankWorkflow(Workflow):
    """
    Does nothing

    """

    def run(self, remote):
        return True

f = open("./blank/mocks_output.txt", "rb")
responses = map(json.loads, f.readlines())
f.close()


def mock_json_values(self, action, data, object_hook=None):
    try:
        resp = responses.pop(0)
        return resp[1]
    except IndexError:
        return None


class BlankWorkflowTest(unittest.TestCase):

    @patch.object(Layer1, 'json_request', mock_json_values)
    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_workflow_registration(self):
        my_config = make_config('RolisTest')

        # Start a workflow
        BlankWorkflowId = my_config.workflow_starter('BlankWorkflow', 1)
        print 'Starting: ', BlankWorkflowId()

        # Run one decision task
        my_config.scan()
        my_config._client.dispatch_next_decision(task_list='a_list')


