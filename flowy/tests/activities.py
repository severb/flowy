import time

from flowy.scanner import swf_activity as activity
from flowy.task import SWFActivity as Activity


@activity(7, name='Identity')
class IdentityActivity(Activity):

    def run(self, n):
        return n


@activity(7, task_list='example_list')
class Double(Activity):

    def run(self, n):
        return n * 2


@activity(7, heartbeat=10)
class Sum(Activity):

    def run(self, *n):
        return sum(n)


@activity(7, schedule_to_close=10)
class Square(Activity):

    def run(self, n):
        return n ** 2


@activity(7, schedule_to_start=20)
class Error(Activity):

    def run(self, msg='err'):
        raise RuntimeError(msg)


@activity(7, start_to_close=30)
class Timeout(Activity):

    def run(self):
        time.sleep(5)


@activity(7)
class Heartbeat(Activity):
    def run(self):
        self.heartbeat()
        self.heartbeat()
        return 'heartbeat'
