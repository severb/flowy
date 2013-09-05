from pyswf.workflow import Workflow, ActivityProxy
from pyswf.client import WorkflowClient


class PrimeTestParallel(Workflow):
    name = 'PrimeTestWorkflow3'
    version = 1

    div = ActivityProxy('Divider2', 1)

    def run(self, n=None):
        n = n if n is not None else 7 * 11
        scheduled = []
        for i in range(2, n/2 + 1):
            scheduled.append(self.div(n, i))
        for s in scheduled:
            if s.result():
                print '%s is divisible by %s' % (n, i)
                break



c = WorkflowClient('SeversTest', 'prime_task_list', [PrimeTestParallel])
c.run()
