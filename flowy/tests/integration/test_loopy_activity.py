import unittest
from mock import create_autospec, MagicMock, patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
import json


@activity_config('RangeActivity', 2, 'a_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class RangeActivity(Activity):
    """
    Return constant value

    """
    def run(self, heartbeat):
        return 3


@activity_config('OperationActivity', 2, 'a_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class OperationActivity(Activity):
    """
    Return double value of parameter

    """
    def run(self, heartbeat, n):
        return 2*n


f = open("./loopy/loopy_activities.txt", "rb")
responses = map(json.loads, f.readlines())
f.close()


def mock_json_values(self, action, data, object_hook=None):
    try:
        resp = responses.pop(0)
        return resp[1]
    except IndexError:
        return None


class SimpleActivityTest(unittest.TestCase):

    @patch.object(Layer1, 'json_request', mock_json_values)
    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_activity(self):
        my_config = make_config('RolisTest')

        # Run one activity task
        my_config.scan()
        my_config._client.dispatch_next_activity(task_list='constant_list')
        my_config._client.dispatch_next_activity(task_list='constant_list')
        my_config._client.dispatch_next_activity(task_list='constant_list')
        my_config._client.dispatch_next_activity(task_list='constant_list')

