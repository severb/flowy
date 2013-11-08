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


@workflow_config('PrimeMaster', 1, 'prime_task_list')
class PrimeMaster(Workflow):

    prime_test = WorkflowProxy(
        name='MyPrime',
        version=2,
    )

    def run(self, remote, n=13):
        r1 = remote.prime_test(n)
        r2 = remote.prime_test(n+1)
        if r1.result() or r2.result():
            print 'one of them is prime!'


if __name__ == '__main__':
    my_config = make_config('SeversTest')

    # Start a workflow
    PrimeMasterWF = my_config.workflow_starter('PrimeMaster', 1)
    print 'Starting: ', PrimeMasterWF(n=22)

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='prime_task_list')
