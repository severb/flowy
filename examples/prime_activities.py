from pyswf.activity import Activity
from pyswf.client import ActivityClient


my_client = ActivityClient.for_domain('SeversTest', 'div_list')


@my_client('NumberDivider', 1)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, n, x):
        return n % x == 0


my_client.start()
