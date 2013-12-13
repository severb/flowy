import unittest
from mock import create_autospec, MagicMock, patch, Mock #, call
from flowy import Workflow
from flowy import make_config, workflow_config
from boto.swf.layer1 import Layer1
import json


@workflow_config('BlankWorkflow', 3, 'a_list', 60, 60)
class BlankWorkflow(Workflow):
    """
    Does nothing

    """

    def run(self, remote):
        print("smt")
        return True

f = open("./blank/mocks_output.txt", "rb")
responses = map(json.loads, f.readlines())
f.close()

mock_json_values = Mock(side_effect=map(lambda x: x[1], responses))


class BlankWorkflowTest(unittest.TestCase):

    @patch.object(Layer1, 'json_request', mock_json_values)
    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_workflow_registration(self):
        my_config = make_config('RolisTest')

        # Start a workflow
        BlankWorkflowId = my_config.workflow_starter('BlankWorkflow', 3)
        print 'Starting: ', BlankWorkflowId()

        my_config.scan()
        my_config._client.dispatch_next_decision('a_list')
        self.assertEqual(mock_json_values.call_count, 4)



