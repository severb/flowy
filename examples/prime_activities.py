from flowy.activity import Activity
from flowy.client import ActivityClient


my_client = ActivityClient.for_domain('SeversTest', 'div_list')


@my_client('NumberDivider', 1, heartbeat=5, start_to_close=60)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, n, x):
        import random
        import time
        for x in range(3):
            s = random.randint(0, 4)
            time.sleep(s)
            self.heartbeat()
        return n % x == 0


my_client.start()
