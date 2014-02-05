from flowy.task import Workflow

from flowy.swf.scanner import workflow
from flowy.swf.task import ActivityProxy


@workflow('MyPrime', 2, 'prime_task_list')
class PrimeTest(Workflow):
    """
    Checks if a number is prime.

    """
    div = ActivityProxy(
        name='NumberDivider',
        version=5,
        heartbeat=5,
        start_to_close=60
    )

    def run(self, n=77):
        for i in range(2, n/2 + 1):
            r = self.div(n, i)
            if r.result():
                return False
        return True


if __name__ == '__main__':
    from boto.swf.layer1 import Layer1

    from flowy import MagicBind
    from flowy.scanner import Scanner
    from flowy.spec import WorkflowSpecCollector
    from flowy.worker import SingleThreadedWorker

    from flowy.swf.spec import WorkflowSpec
    from flowy.swf.poller import DecisionPoller

    swf_client = MagicBind(Layer1(), domain='SeversTest')

    scanner = Scanner(WorkflowSpecCollector(WorkflowSpec, swf_client))
    scanner.scan_workflows()

    poller = DecisionPoller(swf_client, 'prime_task_list')
    worker = SingleThreadedWorker(poller)

    not_registered = scanner.register(worker)
    if not_registered:
        print 'could not register!'
        print not_registered
        import sys
        sys.exit(1)

    worker.run_forever()
