import unittest
from mock import create_autospec, MagicMock, patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
from WorkflowTestCase import load_json_responses


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


@patch.object(Layer1, '__init__', lambda *args: None)
class SimpleActivityTest(unittest.TestCase):

    @load_json_responses("loopy/loopy_activities.txt")
    def test_activity(self, requests):
        my_config = make_config('RolisTest')

        # Run one activity task
        my_config.scan()

        for _ in range(4):
            my_config._client.dispatch_next_activity(task_list='constant_list')
