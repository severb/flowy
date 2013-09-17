from pyswf.activity import Activity
from pyswf.client import ActivityClient



class MyActivity(Activity):

    def run(self, n, x):
        if x == 4:
            raise RuntimeError('Hello')
        return n % x == 0


c = ActivityClient('SeversTest', 'prime_task_list')
c.register('Divider2', '1', MyActivity)
c.run()
