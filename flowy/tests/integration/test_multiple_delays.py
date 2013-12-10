from flowy import Workflow, ActivityProxy, WorkflowProxy
import unittest
from mock import create_autospec, MagicMock, patch
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
import json
from flowy import make_config, workflow_config


@workflow_config('MultipleDelaysWf', 1, 'constant_list', 60, 60)
class MultipleDelaysWf(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SleepyActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        r1 = remote.div(1)
        r2 = remote.div(4)

        print(r1.result())
        print(r2.result())


f = open("./sleepy/sleepy_workflow.txt", "rb")
responses = map(json.loads, f.readlines())
f.close()

requests = []
def mock_json_values(self, action, data, object_hook=None):
    try:
        requests.append((action, data))
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
        DelayWorkflowID = my_config.workflow_starter('MultipleDelaysWf', 1)
        print 'Starting: ', DelayWorkflowID()

        # Run one decision task
        my_config.scan(ignore=["mock"])
        my_config._client.dispatch_next_decision(task_list='constant_list')
        my_config._client.dispatch_next_decision(task_list='constant_list')
        my_config._client.dispatch_next_decision(task_list='constant_list')

