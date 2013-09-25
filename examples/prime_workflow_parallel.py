from pyswf.workflow import Workflow, ActivityProxy
from pyswf.client import WorkflowClient


my_client = WorkflowClient.for_domain('SeversTest', 'prime_task_list')


@my_client('MyPrimeParallel', 2)
class PrimeTestParallel(Workflow):
    """
    Checks if a number is prime in a parallel fashion.

    """

    div = ActivityProxy('Divider2', 1)

    def run(self, n=77, m=3):
        it = iter(range(2, n / 2 + 1))
        ints = zip(*[it] * m)
        for tup in ints:
            scheduled = []
            for divizor in tup:
                scheduled.append(self.div(n, divizor))
            if any(s.result() for s in scheduled):
                return "not prime"
        return "prime"


my_client.start()
