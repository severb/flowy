import importlib
import itertools
import json
import os
import string
import sys
import unittest
import uuid
from pprint import pformat as pf

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError
from boto.swf.layer1 import Layer1

from flowy.boilerplate import start_activity_worker, start_workflow_worker
from flowy.util import MagicBind


test_modules = [
    ('flowy.tests.integration.simple', 'IntegrationTest'),
    ('flowy.tests.integration.dependency', 'IntegrationTest'),
    ('flowy.tests.integration.sequence', 'IntegrationTest'),
    ('flowy.tests.integration.mapreduce', 'IntegrationTest'),
    ('flowy.tests.integration.options', 'IntegrationTest'),
    ('flowy.tests.integration.errors', 'IntegrationTest'),
    ('flowy.tests.integration.heartbeat', 'IntegrationTest'),
    ('flowy.tests.integration.restart', 'IntegrationTest'),
]


class Layer1Playback(Layer1):

    def __len__(self):
        assert len(self.responses) == len(self.requests)
        regs = 0
        for action, data in self.requests:
            if action in ['RegisterWorkflowType', 'DescribeWorkflowType',
                          'RegisterActivityType', 'DescribeActivityType',
                          'RecordActivityTaskHeartbeat']:
                regs += 1
        return (len(self.responses) - regs) / 2

    def __init__(self, log_file):
        self.responses = []
        self.requests = []
        for line in log_file:
            sep, data = line.split('\t', 1)
            if sep == '<<<':
                try:
                    data = json.loads(data)
                except ValueError:
                    pass
                self.responses.append(data)
            else:
                action, request = data.split('\t', 1)
                self.requests.append((action, json.loads(request)))
        log_file.close()
        if not len(self.responses) == len(self.requests):
            raise ValueError('Unbalanced log file.')
        self.responses_i = iter(self.responses)
        self.requests_i = iter(self.requests)

    def json_request(self, action, data, object_hook=None):
        self._normalize_request_dict(data)
        next_action, next_data = next(self.requests_i)
        action_msg = ("The actions don't match."
                      " Expected action: %s. Actual action: %s.")
        assert next_action == action, action_msg % (next_action, action)
        data_msg = ("Request data doesn't match."
                    " Expected data:\n%s\nActual data:\n%s\n")
        assert next_data == data, data_msg % (pf(next_data), pf(data))
        next_response = next(self.responses_i)
        try:
            if next_response.strip() == 'SWFResponseError':
                raise SWFResponseError(None, None)
            if next_response.strip() == 'SWFTypeAlreadyExistsError':
                raise SWFTypeAlreadyExistsError(None, None)
        except AttributeError:
            pass
        return next_response


class TestIntegration(unittest.TestCase):
    def setUp(self):
        self._old_uuid4 = uuid.uuid4
        uuid.uuid4 = itertools.count(1000).next

    def tearDown(self):
        uuid.uuid4 = self._old_uuid4


def make_workflow_runner(log_path, package, domain, task_list):
    def test(self):
        layer1 = Layer1Playback(open(log_path))
        start_workflow_worker(domain, task_list, layer1=layer1,
                              package=package,
                              loop=len(layer1),
                              setup_log=False)
    return test


def make_activity_runner(log_path, package, domain, task_list):
    def test(self):
        layer1 = Layer1Playback(open(log_path))
        start_activity_worker(domain, task_list, layer1=layer1,
                              package=package,
                              loop=len(layer1),
                              setup_log=False)
    return test


for module_name, domain in test_modules:
    try:
        module = importlib.import_module(module_name)
        for i, run in enumerate(module.runs):
            workflow_file_name = module.__name__ + '.%s.workflow.log' % i
            activity_file_name = module.__name__ + '.%s.activity.log' % i
            logs = os.path.join(os.path.dirname(__file__),
                                'integration', 'logs')
            workflow_file = os.path.join(logs, workflow_file_name)
            activity_file = os.path.join(logs, activity_file_name)
            test_module_name = module_name.replace('.', '_')
            workflow_test = 'test_workflow_%s_%s' % (test_module_name, i)
            activity_test = 'test_activity_%s_%s' % (test_module_name, i)
            setattr(TestIntegration, workflow_test,
                    make_workflow_runner(workflow_file, module,
                                         domain, run['task_list']))
            setattr(TestIntegration, activity_test,
                    make_activity_runner(activity_file, module,
                                         domain, run['task_list']))
    except Exception as e:
        print >> sys.stderr, 'Could not load module %s!' % module_name
        print >> sys.stderr, str(e)
