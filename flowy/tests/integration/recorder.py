import argparse
import functools
import importlib
import json
import random
import string
import sys
import threading
import os

from boto.swf.layer1 import Layer1
from flowy.boilerplate import (start_activity_worker, start_workflow_worker,
                               workflow_starter)
from mock import patch


class Layer1WorkflowRecorder(Layer1):

    def __init__(self, f, *args, **kwargs):
        super(Layer1WorkflowRecorder, self).__init__(*args, **kwargs)
        self.f = f
        self.close = False

    def _print_out(self, msg):
        self.f.write('>>>\t%s\n' %  msg)

    def _print_in(self, msg):
        self.f.write('<<<\t%s\n' %  msg)

    def make_request(self, action, body, object_hook=None):
        if self.close:
            sys.exit()
        self._print_out('%s\t%s' % (action, body))
        try:
            result = super(Layer1WorkflowRecorder, self).make_request(
                action, body, object_hook
            )
        except Exception as e:
            self._print_in(e.__class__.__name__)
            raise
        self._print_in(json.dumps(result))
        for decision in json.loads(body).get('decisions', []):
            failed = decision['decisionType'] == 'FailWorkflowExecution'
            com = decision['decisionType'] == 'CompleteWorkflowExecution'
            if failed or com:
                self.close = True
        return result

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("module")
    parser.add_argument("domain")
    args = parser.parse_args()
    module = importlib.import_module(args.module)

    for i, run in enumerate(module.runs):
        workflow_file_name = module.__name__ + '.%s.workflow.log' % i
        activity_file_name = module.__name__ + '.%s.activity.log' % i
        logs = os.path.join(os.path.dirname(__file__), 'logs')
        workflow_client = Layer1WorkflowRecorder(
            open(os.path.join(logs, workflow_file_name), 'w+')
        )
        activity_client = Layer1WorkflowRecorder(
            open(os.path.join(logs, activity_file_name), 'w+')
        )

        with patch('uuid.uuid4') as uuid:
            random.seed(0)
            uuid.return_value = ''.join(random.choice(string.ascii_uppercase +
                                        string.digits) for x in range(10))
            kwargs = {
                'domain': args.domain,
                'name': run['name'],
                'version': run['version'],
                'task_list': run['task_list'],
                'layer1': workflow_client
            }
            if run.get('decision_duration') is not None:
                kwargs['decision_duration'] = run['decision_duration']
            if run.get('workflow_duration') is not None:
                kwargs['workflow_duration'] = run['workflow_duration']
            if run.get('id') is not None:
                kwargs['id'] = run['id']
            if run.get('tags') is not None:
                kwargs['tags'] = run['tags']

            starter = workflow_starter(**kwargs)

            start_activity = functools.partial(
                start_activity_worker,
                domain=args.domain,
                task_list=run['task_list'],
                layer1=activity_client,
                package=module,
            )
            start_workflow = functools.partial(
                start_workflow_worker,
                domain=args.domain,
                task_list=run['task_list'],
                package=module,
                layer1=workflow_client
            )

            activity_thread = threading.Thread(target=start_activity)
            workflow_thread = threading.Thread(target=start_workflow)

            activity_thread.start()
            workflow_thread.start()

            starter.start(*run.get('args', []), **run.get('kwargs', {}))

            workflow_thread.join()
            activity_client.close = True
            activity_thread.join()
