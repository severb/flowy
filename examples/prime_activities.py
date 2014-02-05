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
    from boto.swf.layer1 import Layer1

    from flowy import MagicBind
    from flowy.scanner import Scanner
    from flowy.spec import ActivitySpecCollector
    from flowy.worker import SingleThreadedWorker

    from flowy.swf.spec import ActivitySpec
    from flowy.swf.poller import ActivityPoller

    swf_client = MagicBind(Layer1(), domain='SeversTest')

    scanner = Scanner(ActivitySpecCollector(ActivitySpec, swf_client))
    scanner.scan_activities()

    poller = ActivityPoller(swf_client, 'mr_list')
    worker = SingleThreadedWorker(poller)

    not_registered = scanner.register(worker)
    if not_registered:
        print 'could not register!'
        print not_registered
        import sys
        sys.exit(1)

    worker.run_forever()
