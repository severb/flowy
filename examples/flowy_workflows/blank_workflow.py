from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('BlankWorkflow', 3, 'a_list', task_start_to_close=1,
                 execution_start_to_close=60)
class BlankWorkflow(Workflow):
    """
    Does nothing

    """

    def run(self, remote):
        return True


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    f = open("/home/local/3PILLAR/rszabo/flowy/mocks_output.txt", "w")
    f.close()
    f = open("/home/local/3PILLAR/rszabo/flowy/mocks.txt", "w")
    f.close()

    # Start a workflow
    BlankWorkflowId = my_config.workflow_starter('BlankWorkflow', 3, task_list='b_list')
    print 'Starting: ', BlankWorkflowId()

    from time import sleep
    sleep(0)
    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='b_list')
