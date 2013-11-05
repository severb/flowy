from flowy import Client, Activity
from flowy.swf import SWFClient


class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, heartbeat, n, x):
        import time

        for i in range(3):
            time.sleep(3)
            if not heartbeat():
                print 'abort abort!'
                return
#         time.sleep(10)
#         if not self.heartbeat():
#             print 'abort abort'
#             return
        return n % x == 0


client = Client(SWFClient(domain='SeversTest'))
client.register_activity(
    activity_runner=NumberDivider(),
    name='NumberDivider',
    version=4,
    task_list='div_list',
    heartbeat=5,
    start_to_close=60
)

while 1:
    client.dispatch_next_activity(task_list='div_list')
