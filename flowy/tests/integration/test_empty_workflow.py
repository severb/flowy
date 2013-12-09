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
        print(mock_json_values.call_count)

        # mock_json_values.assert_has_calls([call('StartWorkflowExecution', {'domain': 'RolisTest', 'taskList': {'name': None}, 'childPolicy': None, 'executionStartToCloseTimeout': None, 'input': '{"args": [], "kwargs": {}}', 'workflowType': {'version': '1', 'name': 'BlankWorkflow'}, 'taskStartToCloseTimeout': None, 'workflowId': '22c8d7f5-b3b6-410a-ab15-ecf48804bc69', 'tagList': None}),
        #          call('RegisterWorkflowType', {'defaultExecutionStartToCloseTimeout': '60', 'domain': 'RolisTest', 'version': '1', 'name': 'BlankWorkflow', 'defaultChildPolicy': 'TERMINATE', 'defaultTaskStartToCloseTimeout': '60', 'defaultTaskList': {'name': 'a_list'}, 'description': None}),
        #          call('PollForDecisionTask', {'nextPageToken': None, 'domain': 'RolisTest', 'taskList': {'name': 'a_list'}, 'reverseOrder': True, 'maximumPageSize': None, 'identity': None}),
        #          call('RespondDecisionTaskCompleted', {'executionContext': None, 'decisions': [{'completeWorkflowExecutionDecisionAttributes': {'result': 'true'}, 'decisionType': 'CompleteWorkflowExecution'}], 'taskToken': u'AAAAKgAAAAEAAAAAAAAAAlGMFJwGm+285RxpzMUDfbEDSSJNfXqXab5CR8o784gj+Kga1yeiSpANp/h9J8cu3Wndhy4cE08qz/xhm9bKqZECH/W+HCFg7yoWRtVKYKTPv6IWWlPts4viUto5eAL+U5J5rohSaMaVs6M6z5NVxUptKg3NgMYYzlnCr9me0RzpS6P6E5FOu7La3xl9hjjFQz9B2ibcPP8/ynnCzBzekSODS8DoxRbHdT6eF/+yoV4/CbAra/KUrqorC7RUaG+l6oRn++asWqzou5l8ZNBgsTnR5jCkAs4/UvAGETCmuIITp025h3cGEC9In1v1NY49hw=='})
        #     ])


