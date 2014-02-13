import os
import unittest
import json
from boto.swf.layer1 import Layer1
import workflows
from flowy.swf.boilerplate import start_workflow_worker
from itertools import cycle
import random
from mock import patch
import string

class MockLayer1(Layer1):

    def __init__(self, responses, requests):
        self.responses = cycle(responses)
        self.requests = cycle(requests)

    def json_request(self, action, data, object_hook=None):
        self._normalize_request_dict(data)
        nxt_req = next(self.requests)
        assert nxt_req[0] == action, ('Difference expected %s, got %s'
                                      % (nxt_req[0], action))
        assert nxt_req[1] == data
        nxt_resp = next(self.responses)
        return nxt_resp


def make(file_name):
    f = open(os.path.join(here, 'logs', file_name))
    responses = []
    requests = []
    for line in f:
        line = line.split('\t')
        if line[0] == '<<<':
            res = json.loads(line[1])
            responses.append(res)
        else:
            requests.append((line[1], json.loads(line[2])))

    mock_layer1 = MockLayer1(responses, requests)

    @patch('uuid.uuid4')
    def test(self, uuid):
        random.seed(0)
        uuid.return_value = ''.join(random.choice(string.ascii_uppercase +
                                    string.digits) for x in range(10))
        start_workflow_worker('IntegrationTest', 'example_list',
                              layer1=mock_layer1,
                              reg_remote=False,
                              package=workflows,
                              loop=len(responses))
    f.close()
    return test


class ExamplesTest(unittest.TestCase):
    pass


here = os.path.dirname(__file__)

for file_name in os.listdir(os.path.join(here, 'logs')):
    test_name = 'test_' + file_name.rsplit('.', 1)[0]
    setattr(ExamplesTest, test_name, make(file_name))
