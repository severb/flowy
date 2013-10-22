from flowy.activity import Activity
from flowy.client import activity_client


@activity_client('NumberDivider', 4, 'div_list', heartbeat=5, start_to_close=60)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, n, x):
        import time

        for i in range(3):
            time.sleep(3)
            if not self.heartbeat():
                print 'abort abort!'
                return
        time.sleep(10)
        if not self.heartbeat():
            print 'abort abort'
            return
        return n % x == 0


activity_client.start_on('SeversTest', 'div_list')
