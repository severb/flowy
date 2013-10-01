from pyswf.workflow import Workflow, ActivityProxy
from pyswf.client import WorkflowClient


my_client = WorkflowClient.for_domain('SeversTest', 'prime_task_list')


@my_client('MyPrime', 1)
class PrimeTest(Workflow):
    """
    Checks if a number is prime.

    """
    div = ActivityProxy('NumberDivider', 1)

    def run(self, n=77):

        for i in range(2, n/2 + 1):
            r = self.div(n, i)
            if r.result():
                return 'not prime'
        return 'prime'


my_client.start()
