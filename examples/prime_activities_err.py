from flowy.activity import Activity
from flowy.client import ActivityClient


my_client = ActivityClient.for_domain('SeversTest', 'prime_task_list')


@my_client('NumberDivider', 2)
class NumberDivider(Activity):
    """
    Divide numbers. Raises an exception.

    """
    def run(self, n, x):
        if x == 4:
            raise RuntimeError('Hello')
        return n % x == 0


my_client.start()
