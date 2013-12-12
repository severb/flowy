from flowy import Workflow, ActivityProxy, WorkflowProxy
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

        print(r1.result())
        print(r2.result())



if __name__ == '__main__':
    my_config = make_config('RolisTest')

    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks_output.txt", "w")
    # f.close()
    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks.txt", "w")
    # f.close()

    # Start a workflow
    DelayWorkflowID = my_config.workflow_starter('MultipleDelaysWf', 1)
    print 'Starting: ', DelayWorkflowID()

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='constant_list')
