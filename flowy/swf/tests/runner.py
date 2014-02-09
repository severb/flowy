import json
import sys

from boto.swf.layer1 import Layer1
from flowy.swf.boilerplate import start_activity_worker, start_workflow_worker


class Layer1WorkflowRecorder(Layer1):

    def make_request(self, action, body, object_hook=None):
        print '>>>\t' + action + '\t' + body
        result = super(Layer1WorkflowRecorder, self).make_request(
            action, body, object_hook
        )
        print '<<<\t' + json.dumps(result)
        return result

if __name__ == '__main__':
    client = Layer1WorkflowRecorder()
    if 'activity' in sys.argv:
        from flowy.swf.tests import activities
        start_activity_worker(
            domain='SeversTest',
            task_list='example_list',
            layer1=client,
            package=activities
        )
    else:
        from flowy.swf.tests import workflows
        start_workflow_worker(
            domain='SeversTest',
            task_list='example_list',
            layer1=client,
            package=workflows
        )
