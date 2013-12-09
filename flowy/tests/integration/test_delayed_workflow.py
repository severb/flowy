import unittest
from mock import create_autospec, MagicMock, patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
import json

@workflow_config('DelayedWorkflow', 1, 'constant_list', 60, 60)
class DelayedWorkflow(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SimpleActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        with remote.option(delay=10):
            print("here")
            r = remote.div()
            r.result()
            print("done")

f = open("./delayed/delayed_workflow.txt", "rb")
responses = map(json.loads, f.readlines())
f.close()

def mock_json_values(self, action, data, object_hook=None):
    try:
        resp = responses.pop(0)
        print(resp)
        return resp[1]
    except IndexError:
        return None


class SimpleWorflowTest(unittest.TestCase):

    @patch.object(Layer1, 'json_request', mock_json_values)
    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_workflow(self):
        my_config = make_config('RolisTest')


        # Start a workflow
        DelayedWorkflowID = my_config.workflow_starter('DelayedWorkflow', 1)
        print 'Starting: ', DelayedWorkflowID()

        # Run one decision task
        my_config.scan()
        my_config._client.dispatch_next_decision(task_list='constant_list')
        my_config._client.dispatch_next_decision(task_list='constant_list')
        # my_config._client.dispatch_next_decision(task_list='constant_list')

