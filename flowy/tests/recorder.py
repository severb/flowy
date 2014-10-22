from __future__ import print_function

import argparse
import functools
import importlib
import itertools
import json
import os
import sys
import threading
import time

from boto.swf.layer1 import Layer1

from flowy.boilerplate import start_activity_worker
from flowy.boilerplate import start_workflow_worker
from flowy.boilerplate import workflow_starter


class Layer1Recorder(Layer1):

    def __init__(self, f, *args, **kwargs):
        super(Layer1Recorder, self).__init__(*args, **kwargs)
        self.f = f
        self.close = False
        self.run_id = None
        self.task_token = None

    def _print_out(self, msg):
        m = '>>>\t%s' % msg
        self.f.write("%s\n" % m)
        print(("\t%s..." if len(m) > 79 else "\t%s") % m[:79])

    def _print_in(self, msg):
        m = '<<<\t%s' % msg
        self.f.write("%s\n" % m)
        print(("\t%s..." if len(m) > 79 else "\t%s") % m[:79])

    def make_request(self, action, body, object_hook=None):
        if self.close:
            sys.exit()
        self._print_out('%s\t%s' % (action, body))
        try:
            result = super(Layer1Recorder, self).make_request(
                action, body, object_hook
            )
            if result is not None:
                run_id = result.get('workflowExecution', {}).get('runId')
                if self.run_id is None:
                    self.run_id = run_id
                if run_id == self.run_id:
                    self.task_token = result.get('taskToken')
        except Exception as e:
            self._print_in(e.__class__.__name__)
            raise
        self._print_in(json.dumps(result))
        loaded_body = json.loads(body)
        for decision in loaded_body.get('decisions', []):
            dt = decision['decisionType']
            if dt in ['FailWorkflowExecution', 'CompleteWorkflowExecution']:
                if loaded_body['taskToken'] == self.task_token:
                    self.close = True
        return result

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("module")
    parser.add_argument("domain")
    args = parser.parse_args()
    module = importlib.import_module(args.module)

    print('Found %s different runs in %s.' % (len(module.runs), module))
    for i, run in enumerate(module.runs):
        print('Iteration %s:' % i)
        workflow_file_name = module.__name__ + '.%s.workflow.log' % i
        activity_file_name = module.__name__ + '.%s.activity.log' % i
        logs = os.path.join(os.path.dirname(__file__), 'integration', 'logs')
        workflow_client = Layer1Recorder(
            open(os.path.join(logs, workflow_file_name), 'w+')
        )
        activity_client = Layer1Recorder(
            open(os.path.join(logs, activity_file_name), 'w+')
        )

        # patch uuid4
        import uuid
        _old_uuid4 = uuid.uuid4
        uuid.uuid4 = itertools.count(1000).next

        kwargs = {
            'domain': args.domain,
            'name': run['name'],
            'version': run['version'],
            'task_list': run['task_list'],
            #'layer1': workflow_client
            # can't log the workflow starter since it will interfere with
            # the logging from the workflow worker which must start first
            # in order to do the registration
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
            identity='ATestID'
        )
        start_workflow = functools.partial(
            start_workflow_worker,
            domain=args.domain,
            task_list=run['task_list'],
            package=module,
            layer1=workflow_client,
            identity='WTestID'
        )

        activity_thread = threading.Thread(target=start_activity)
        workflow_thread = threading.Thread(target=start_workflow)

        print('Starting activity worker thread.')
        activity_thread.start()
        print('Starting workflow worker thread.')
        workflow_thread.start()

        print('Waiting 5 seconds for everything to register...')
        time.sleep(5)

        print('Starting workflow.')
        # don't let the starter consume the first uuid4
        with starter.options(id='testintegration'):
            starter.start(*run.get('args', []), **run.get('kwargs', {}))

        print('Waiting for the workflow thread...')
        workflow_thread.join()
        print('Waiting 5 seconds for timed-out tasks to finish...')
        time.sleep(5)
        activity_client.close = True
        print('Waiting for the activity thread (this may take a while)...')
        activity_thread.join()

        # patch uuid4
        uuid.uuid4 = _old_uuid4
