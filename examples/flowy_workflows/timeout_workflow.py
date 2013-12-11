from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('TimeoutWf', 2, 'constant_list', 1, 2)
class TimeoutWf(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='SleepyActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        r = remote.div(5)
        print(r.result())
        return True


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks_output.txt", "w")
    # f.close()
    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks.txt", "w")
    # f.close()

    # Start a workflow
    SimpleWorkflowID = my_config.workflow_starter('TimeoutWf', 2)
    print 'Starting: ', SimpleWorkflowID()

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='constant_list')
