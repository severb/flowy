from flowy import Workflow, ActivityProxy, WorkflowProxy
from WorkflowTestCase import WorkflowTestCase, load_json_responses
import unittest
from mock import patch
from flowy import make_config, workflow_config, activity_config
from boto.swf.layer1 import Layer1
from flowy import make_config, workflow_config


@workflow_config('MultipleDelaysWf', 1, 'constant_list', 60, 60)
class MultipleDelaysWf(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SleepyActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        r1 = remote.div(1)
        r2 = remote.div(4)

        r1.result()
        r2.result()


class SimpleWorflowTest(WorkflowTestCase):

    @load_json_responses("sleepy/sleepy_workflow.txt")
    def test_sleepy_workflow(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        DelayWorkflowID = my_config.workflow_starter('MultipleDelaysWf', 1)
        print 'Starting: ', DelayWorkflowID()

        # Run one decision task
        my_config.scan(ignore=["mock"])

        for _ in range(3):
            my_config._client.dispatch_next_decision(task_list='constant_list')

        self.assertCompletedWorkflow(requests)

    @load_json_responses("sleepy/sleepy_skip_history.txt")
    def test_history_skip_workflow(self, requests):
        my_config = make_config('RolisTest')

        # Start a workflow
        DelayWorkflowID = my_config.workflow_starter('MultipleDelaysWf', 1)
        print 'Starting: ', DelayWorkflowID()

        # Run one decision task
        my_config.scan(ignore=["mock"])

        for _ in range(3):
            my_config._client.dispatch_next_decision(task_list='constant_list')

        self.assertCompletedWorkflow(requests)
