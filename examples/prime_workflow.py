from pyswf.workflow import Workflow, ActivityProxy, ActivityTimedout
from pyswf.client import WorkflowClient


my_client = WorkflowClient.for_domain('SeversTest', 'prime_task_list')


@my_client('MyPrime', 1)
class PrimeTest(Workflow):
    """
    Checks if a number is prime.

    """
    div = ActivityProxy('NumberDivider', 1, task_list='div_list')

    def run(self, n=77):

        for i in range(2, n/2 + 1):
            with self.options(error_handling=True, retry=1, heartbeat=i * 10, schedule_to_close=5, schedule_to_start=5, start_to_close=5):
                r = self.div(n, i)
            try:
                if r.result():
                    return 'not prime'
            except ActivityTimedout:
                return 'timed out!'
        return 'prime'


my_client.start()
