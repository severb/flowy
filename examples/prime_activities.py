from boto.swf.layer1 import Layer1

from flowy.swf.scanner import activity
from flowy.task import Task


@activity('NumberDivider', 5, 'mr_list', heartbeat=5, start_to_close=60)
class NumberDivider(Task):
    """
    Divide numbers.

    """
    def run(self, heartbeat, n, x):
        import time
        for i in range(3):
            print '.' * 10
            time.sleep(3)
            if not heartbeat():
                print 'cleanup'
                return
        print 'returning'
        return n % x == 0


if __name__ == '__main__':

    from flowy.swf.spec import ActivitySpec
    from flowy.swf.client import DomainBoundClient, ActivityPollerClient

    from flowy.scanner import Scanner
    from flowy.spec import ActivitySpecCollector
    from flowy.worker import SingleThreadedWorker

    swf_client = DomainBoundClient(Layer1(), domain='SeversTest')

    scanner = Scanner(ActivitySpecCollector(ActivitySpec, swf_client))
    scanner.scan_activities()

    activity_poller = ActivityPollerClient(swf_client, 'mr_list')
    worker = SingleThreadedWorker(activity_poller)

    not_registered = scanner.register(worker)
    if not_registered:
        print 'could not register!'
        import sys
        sys.exit(1)

    worker.run_forever()
