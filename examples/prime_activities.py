from pyswf.activity import Activity
from pyswf.client import ActivityClient


my_client = ActivityClient.for_domain('SeversTest', 'prime_task_list')


@my_client('NumberDivider', 2)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, n, x):
        return n % x == 0


my_client.start()
