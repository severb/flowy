from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('MyPrime', 2, 'prime_task_list')
class PrimeTest(Workflow):
    """
    Checks if a number is prime.

    """
    div = ActivityProxy(
        name='NumberDivider',
        version=4,
        task_list='div_list',
        heartbeat=5,
        start_to_close=60
    )

    def run(self, remote, n=77):
        for i in range(2, n/2 + 1):
            r = remote.div(n, i)
            if r.result():
                return False
        return True


@workflow_config('RestartMaster', 1, 'restart_task_list')
class RestartMaster(Workflow):

    restart_wf = WorkflowProxy(
        name='RestartWorkflow',
        version=1,
    )

    def run(self, remote):
        return remote.restart_wf().result()


@workflow_config('RestartWorkflow', 1, 'restart_task_list')
class Restart(Workflow):

    div = ActivityProxy(
        name='NumberDivider',
        version=4,
        task_list='div_list',
        heartbeat=5,
        start_to_close=60
    )

    def run(self, remote, n=13):
        if n < 15:
            remote.restart(n+1)
        return remote.div(n, 5).result()


if __name__ == '__main__':
    my_config = make_config('SeversTest')

    # Start a workflow
    WF = my_config.workflow_starter('RestartMaster', 1)
    print 'Starting: ', WF()

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='restart_task_list')
