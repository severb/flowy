import unittest
from mock import patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, activity_config
from boto.swf.layer1 import Layer1
from WorkflowTestCase import load_json_responses


@activity_config('SimpleActivity', 1, 'constant_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class SimpleActivity(Activity):
    """
    Return constant value

    """
    def run(self, heartbeat):
        return 2


@patch.object(Layer1, '__init__', lambda *args: None)
class SimpleActivityTest(unittest.TestCase):

    @load_json_responses("simple/mock_activity_output.txt")
    def test_activity(self, requests):
        my_config = make_config('RolisTest')

        # Run one activity task
        my_config.scan()
        my_config._client.dispatch_next_activity(task_list='constant_list')

        print(requests)

