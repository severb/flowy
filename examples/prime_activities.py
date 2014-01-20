from boto.swf.layer1 import Layer1

from flowy.worker import SingleThreadedWorker
from flowy.spec import RemoteCollectorSpec, RemoteScannerSpec, SWFActivitySpec
from flowy.task import Task
from flowy.client import SWFDomainBoundClient, SWFActivityPollerClient

# from flowy.runtime import Heartbeat, ActivityResult


my_activity_scanner = RemoteScannerSpec(
    RemoteCollectorSpec(spec_factory=SWFActivitySpec)
)


@my_activity_scanner(
    'NumberDivider', 5, 'mr_list', heartbeat=5, start_to_close=60
)
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
    swf_client = SWFDomainBoundClient(Layer1(), domain='SeversTest')
    my_activity_scanner.bind_client(swf_client)
    activity_worker = SingleThreadedWorker(
        SWFActivityPollerClient(swf_client, 'mr_list')
    )
    all_registered = my_activity_scanner.register(activity_worker)
    if not all_registered:
        print 'could not register!'
        import sys
        sys.exit(1)
    activity_worker.run_forever()
