from pyswf.activity import activity
from pyswf.client import ActivityClient


@activity('Divider', 1)
def divider(n, x):
    return n % x == 0


c = ActivityClient([divider])
c.run('SeversTest', 'prime_task_list')
