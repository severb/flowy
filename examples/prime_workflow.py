from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('MyPrime', 2, 'prime_task_list')
class PrimeTest(Workflow):
    """
    Checks if a number is prime.

    """
    div = ActivityProxy(
        name='NumberDivider',
        version=5,
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
        with remote.options(delay=10):
            r1 = remote.prime_test(n)
        with remote.options(delay=20):
            r2 = remote.prime_test(n+1)
        if r1.result() or r2.result():
            print 'one of them is prime!'


if __name__ == '__main__':
    my_config = make_config('SeversTest')

    # Start a workflow
    WF = my_config.workflow_starter('MyPrime', 2)
    print 'Starting: ', WF(n=22)

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='prime_task_list')
