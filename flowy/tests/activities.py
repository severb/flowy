import time

from flowy.scanner import swf_activity as activity
from flowy.task import SWFActivity as Activity


@activity(77, 'example_list', name='Identity')
class IdentityActivity(Activity):

    def run(self, n):
        return n


@activity(77, 'example_list')
class Double(Activity):

    def run(self, n):
        return n * 2


@activity(77, 'example_list')
class Sum(Activity):

    def run(self, *n):
        return sum(n)


@activity(77, 'example_list')
class Square(Activity):

    def run(self, n):
        return n ** 2


@activity(77, 'example_list')
class Error(Activity):

    def run(self, msg='err'):
        raise RuntimeError(msg)


@activity(77, 'example_list', start_to_close=2)
class Timeout(Activity):

    def run(self):
        time.sleep(5)


@activity(77, 'example_list')
class Heartbeat(Activity):
    def run(self, n):
        self.heartbeat()
        self.heartbeat()
        return 'heartbeat'
