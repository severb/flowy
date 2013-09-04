from pyswf.workflow import Workflow, ActivityProxy
from pyswf.client import WorkflowClient


class PrimeTest(Workflow):
    name = 'PrimeTestWorkflow'
    version = 1

    div = ActivityProxy('Divider', 1)

    def run(self):
        n = 7 * 11
        for i in range(2, n/2):
            if self.div(n, i).result():
                print '%s is divisible by %s' % (n, i)
                break



c = WorkflowClient([PrimeTest])
c.run('SeversTest', 'prime_task_list')
