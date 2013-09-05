from pyswf.activity import activity
from pyswf.client import ActivityClient


@activity('Divider2', 1)
def divider(n, x):
    return n % x == 0


c = ActivityClient('SeversTest', 'prime_task_list', [divider])
c.run()
