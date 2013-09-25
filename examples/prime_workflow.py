from pyswf.workflow import Workflow, ActivityProxy
from pyswf.client import WorkflowClient


my_client = WorkflowClient.for_domain('SeversTest', 'prime_task_list')


@my_client('MyPrime', 2)
class PrimeTest(Workflow):
    """
    Checks if a number is prime.

    """
    div = ActivityProxy('NumberDivider', 2)

    def run(self, n=None):
        n = n if n is not None else 7 * 11
        for i in range(2, n/2 + 1):
            r = self.div(n, i)
            if r.result():
                return 'not prime'
        return 'prime'


my_client.start()
