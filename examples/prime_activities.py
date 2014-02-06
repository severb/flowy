from flowy.swf.boilerplate import start_activity_worker
from flowy.swf.scanner import activity
from flowy.task import Activity


@activity('NumberDivider', 5, 'mr_list', heartbeat=5, start_to_close=60)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, n, x):
        print n, x
        import time
        for i in range(3):
            print '.' * 10
            time.sleep(3)
            if not self.heartbeat():
                print 'cleanup'
                return
        print 'returning'
        return n % x == 0


if __name__ == '__main__':
    start_activity_worker('SeversTest', 'mr_list')
