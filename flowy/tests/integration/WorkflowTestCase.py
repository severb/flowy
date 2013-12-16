import unittest
from boto.swf.layer1 import Layer1
from mock import patch
import os
import json
from functools import partial


def mock_json_values(action, data, object_hook=None, requests=[],
                     responses=[]):
    try:
        requests.append((action, data))
        resp = responses.pop(0)
        return resp[1]
    except IndexError:
        return None


def load_json_responses(file_name):
    """
    Patch Layer1 make_request to return the contents of file_name
    """
    base = os.path.dirname(__file__)
    filepath = os.path.abspath(os.path.join(base, file_name))
    f = open(filepath, "rb")
    responses = map(json.loads, f.readlines())
    f.close()
    requests = []
    f = partial(mock_json_values, requests=requests, responses=responses)
    def decorator(test_item):
        @patch.object(Layer1, 'json_request', f)
        def w(self):
            return test_item(self, requests)
        return w
    return decorator


class WorkflowTestCase(unittest.TestCase):

    def setUp(self):
        self.patcher = patch.object(Layer1, '__init__', lambda *args: None)
        self.patcher.start()

    def assertCompletedWorkflow(self, requests):
        self.assertEqual(requests[-1][1]['decisions'][0]['decisionType'],
                         'CompleteWorkflowExecution')
        self.assertEqual(len(requests[-1][1]['decisions']), 1)

    def assertFailedWorkflow(self, requests):
        self.assertEqual(requests[-1][1]['decisions'][0]['decisionType'],
                         'FailWorkflowExecution')
        self.assertEqual(len(requests[-1][1]['decisions']), 1)

    def tearDown(self):
        self.patcher.stop()

