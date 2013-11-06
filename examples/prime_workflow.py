from flowy import make_config, Workflow, ActivityProxy, workflow_config


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
                return 'not prime'
        return 'prime'


if __name__ == '__main__':
    my_config = make_config('SeversTest')
    my_config.scan()
    my_config.start_workflow_loop(task_list='prime_task_list')
