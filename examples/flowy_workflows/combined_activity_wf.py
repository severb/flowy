from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('LoopyWorkflow', 2, 'a_list', 60, 60)
class LoopyWorkflow(Workflow):
    """
    Executes two activites. One of them is passed as argument the other one,
    without calling result.

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
        r2 = remote.op(remote.loop())
        print(r2.result())


if __name__ == '__main__':
    my_config = make_config('RolisTest')


    # Start a workflow
    LoopyWorkflowID = my_config.workflow_starter('LoopyWorkflow', 2)
    print 'Starting: ', LoopyWorkflowID()

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='a_list')
