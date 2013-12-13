import unittest
from functools import partial
from mock import create_autospec, MagicMock, patch
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
import json


@workflow_config('LoopyWorkflow', 3, 'a_list', 60, 60)
class LoopyWorkflow(Workflow):
    """
    Executes two activites. One of them returns the number of times to loop
    over the other one

    """
    loop = ActivityProxy(
        name='RangeActivity',
        version=2,
        task_list='a_list',
    )
    op = ActivityProxy(
        name='OperationActivity',
        version=2,
        task_list='a_list'
    )

    def run(self, remote):
        r = remote.loop()
        results = []
        for i in range(r.result()):
            res = remote.op(i)
            results.append(res)
        for res in results:
            res.result()
        return True


class LoopyWorflowTest(unittest.TestCase):

    @patch.object(Layer1, '__init__', lambda *args: None)
    def test_workflow(self):
        f = open("./loopy/loopy_parallel.txt", "rb")
        responses = map(json.loads, f.readlines())
        f.close()
		
        requests = []
		
        f = partial(mock_json_values, requests=requests, responses=responses)
        with patch.object(Layer1, 'json_request', f):
		    my_config = make_config('RolisTest')


		    # Start a workflow
		    LoopyWorkflowID = my_config.workflow_starter('LoopyWorkflowID', 3)
		    print 'Starting: ', LoopyWorkflowID()

		    # Run one decision task
		    my_config.scan()
		    my_config._client.dispatch_next_decision(task_list='a_list')
		    my_config._client.dispatch_next_decision(task_list='a_list')
		    my_config._client.dispatch_next_decision(task_list='a_list')
		    my_config._client.dispatch_next_decision(task_list='a_list')
		    my_config._client.dispatch_next_decision(task_list='a_list')
