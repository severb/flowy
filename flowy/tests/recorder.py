import argparse
import importlib
import json
import random
import string
import sys
import threading
import functools

from boto.swf.layer1 import Layer1
from flowy.boilerplate import start_activity_worker, start_workflow_worker
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
        else:
            self._print_in(json.dumps(result))
            for decision in json.loads(body).get('decisions', []):
                failed = decision['decisionType'] == 'FailWorkflowExecution'
                com = decision['decisionType'] == 'CompleteWorkflowExecution'
                af = decision['decisionType'] == 'RespondActivityTaskCompleted'
                afail = decision['decisionType'] == 'RespondActivityTaskFailed'
                if failed or com or af or afail:
                    self.close = True
            return result

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("module")
    parser.add_argument("wfile")
    parser.add_argument("afile")
    parser.add_argument("--register", type=bool, default=True)

    args = parser.parse_args()

    module = importlib.import_module(args.module)

    workflow_client = Layer1WorkflowRecorder(open(args.wfile, 'w+'))
    activity_client = Layer1WorkflowRecorder(open(args.afile, 'w+'))

    with patch('uuid.uuid4') as uuid:
        random.seed(0)
        uuid.return_value = ''.join(random.choice(string.ascii_uppercase +
                                    string.digits) for x in range(10))
        start_activity = functools.partial(
            start_activity_worker,
            domain='IntegrationTest',
            task_list='example_list',
            layer1=activity_client,
            package=module,
            reg_remote=args.register
        )
        start_workflow = functools.partial(
            start_workflow_worker,
            domain='IntegrationTest',
            task_list='example_list',
            layer1=workflow_client,
            package=module,
            reg_remote=args.register
        )

        activity_thread = threading.Thread(target=start_activity)
        workflow_thread = threading.Thread(target=start_workflow)

        activity_thread.start()
        workflow_thread.start()

        activity_thread.join()
        workflow_thread.join()
