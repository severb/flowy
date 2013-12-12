from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('DelayWorkflow', 1, 'constant_list', 60, 60)
class DelayWorkflow(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SimpleActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        print("what?")
        with remote.options(delay=5):
            r = remote.div()
            print(r.result())


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks_output.txt", "w")
    # f.close()
    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks.txt", "w")
    # f.close()

    # Start a workflow
    DelayWorkflowID = my_config.workflow_starter('DelayWorkflow', 1)
    print 'Starting: ', DelayWorkflowID()

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='constant_list')
