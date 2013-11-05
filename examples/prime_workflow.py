from flowy import Client, Workflow, ActivityProxy
from flowy.swf import SWFClient


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

client = Client(SWFClient(domain='SeversTest'))
client.register_workflow(PrimeTest(), 'MyPrime', 2, 'prime_task_list')

while 1:
    client.dispatch_next_decision('prime_task_list')
