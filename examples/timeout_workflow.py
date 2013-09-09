import time

from pyswf.workflow import Workflow, ActivityProxy
from pyswf.activity import activity, ActivityTimedout
from pyswf.client import WorkflowClient, ActivityClient


class MyWorkflow(Workflow):

    name = 'TimeoutTest'
    version = 1

    f1 = ActivityProxy('Nothing', 1)

    def run(self):

        # with self.context(retry=3):
        #       r = self.f1()
        # with self.context(retry=5):
        #       x = self.f1()
        # with self.context(retry=2, start_to_end_timeout=360):
        #       y = self.f1()

        for x in range(3):
            r = 1
            try:
                a = self.f1()
                b = self.f1()
                c = self.f1()
                [a.result(), b.result(), c.result()]
            except ActivityTimedout:
                pass
        return 1


@activity('Nothing', 1)
def wait_activity():
    time.sleep(100)


if __name__ == '__main__':
    import sys
    c = ActivityClient('SeversTest', 'timeoutq', [wait_activity])
    if 'workflow' in sys.argv:
        c = WorkflowClient('SeversTest', 'timeoutq', [MyWorkflow])
    c.run()
