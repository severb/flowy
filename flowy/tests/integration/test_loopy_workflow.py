import unittest
from mock import create_autospec, MagicMock, patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
import json


f = open("./loopy/loopy_worflow.txt", "rb")
responses = map(json.loads, f.readlines())
print(len(responses))
f.close()

def mock_json_values(self, action, data, object_hook=None):
    try:
        resp = responses.pop(0)
        # print(resp)
        return resp[1]
    except IndexError:
        return None


@workflow_config('LoopyWorkflow', 2, 'a_list', 60, 60)
class LoopyWorkflow(Workflow):
    """
    Executes two activites. One of them returns the number of times to loop
    over the other one

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
        for i in range(r.result()):
            res = remote.op(i)
        return True


class LoopyWorflowTest(unittest.TestCase):

    @patch.object(Layer1, 'json_request', mock_json_values)
    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_workflow(self):
        my_config = make_config('RolisTest')


        # Start a workflow
        LoopyWorkflowID = my_config.workflow_starter('LoopyWorkflowID', 1)
        print 'Starting: ', LoopyWorkflowID()

        # Run one decision task
        my_config.scan()
        my_config._client.dispatch_next_decision(task_list='a_list')
        my_config._client.dispatch_next_decision(task_list='a_list')
        my_config._client.dispatch_next_decision(task_list='a_list')
        my_config._client.dispatch_next_decision(task_list='a_list')
        my_config._client.dispatch_next_decision(task_list='a_list')
