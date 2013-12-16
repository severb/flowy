import unittest
from mock import patch
from WorkflowTestCase import WorkflowTestCase, load_json_responses
from flowy import Workflow, ActivityProxy, Activity
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1


@workflow_config('SimpleWorkflow', 1, 'constant_list', 60, 60)
class SimpleWorkflow(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SimpleActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        r = remote.div()
        r.result()
        return True


class SimpleWorflowTest(WorkflowTestCase):

    @load_json_responses("simple/mocks_workflow_output.txt")
    def test_workflow(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        SimpleWorkflowID = my_config.workflow_starter('SimpleWorkflow', 1)
        print 'Starting: ', SimpleWorkflowID()

        # Run one decision task
        my_config.scan(ignore=["mock"])
        my_config._client.dispatch_next_decision(task_list='constant_list')
        my_config._client.dispatch_next_decision(task_list='constant_list')
        self.assertCompletedWorkflow(requests)


    @load_json_responses("errors/error_workflow.txt")
    def test_error_workflow(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        SimpleWorkflowID = my_config.workflow_starter('SimpleWorkflow', 1)
        print 'Starting: ', SimpleWorkflowID()

        # Run one decision task
        my_config.scan(ignore=["mock"])
        my_config._client.dispatch_next_decision(task_list='constant_list')
        my_config._client.dispatch_next_decision(task_list='constant_list')

        self.assertFailedWorkflow(requests)
