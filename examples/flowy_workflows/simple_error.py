from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('ErrorWorkflow', 1, 'constant_list', 30, 30)
class ErrorWorkflow(Workflow):
    """
    Does nothing

    """
    div = ActivityProxy(
        name='ErrorActivity',
        version=1,
        task_list='constant_list',
    )

    def run(self, remote):
        with remote.options(error_handling=True):
            try:
                r = remote.div(True)
            except Exception:
                print("Caught exception")
        return True


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks_output.txt", "w")
    # f.close()
    # f = open("/home/local/3PILLAR/rszabo/flowy/mocks.txt", "w")
    # f.close()

    # Start a workflow
    SimpleWorkflowID = my_config.workflow_starter('ErrorWorkflow', 1)
    print 'Starting: ', SimpleWorkflowID()

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='constant_list')
