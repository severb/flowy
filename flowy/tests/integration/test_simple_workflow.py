import unittest
from mock import create_autospec, MagicMock, patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
import json

@workflow_config('SimpleWorkflow', 1, 'constant_list', 60, 60)
class SimpleWorkflow(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SimpleActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        r = remote.div()
        r.result()
        return True

f = open("./simple/mocks_workflow_output.txt", "rb")
responses = map(json.loads, f.readlines())
f.close()

def mock_json_values(self, action, data, object_hook=None):
    try:
        resp = responses.pop(0)
        return resp[1]
    except IndexError:
        return None


class SimpleWorflowTest(unittest.TestCase):

    @patch.object(Layer1, 'json_request', mock_json_values)
    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_workflow(self):
        my_config = make_config('RolisTest')


        # Start a workflow
        SimpleWorkflowID = my_config.workflow_starter('SimpleWorkflow', 1)
        print 'Starting: ', SimpleWorkflowID()

        # Run one decision task
        my_config.scan()
        my_config._client.dispatch_next_decision(task_list='constant_list')
        my_config._client.dispatch_next_decision(task_list='constant_list')

