from flowy.swf.boilerplate import start_workflow_worker
from flowy.swf.scanner import workflow
from flowy.swf.task import ActivityProxy
from flowy.task import Workflow


@workflow('MyPrime', 2, 'prime_task_list')
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

    def run(self, n=77):
        for i in range(2, n/2 + 1):
            r = self.div(n, i)
            if r.result():
                return False
        return True


if __name__ == '__main__':
    start_workflow_worker('SeversTest', 'prime_task_list')
