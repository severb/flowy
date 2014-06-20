import time

from flowy.scanner import swf_activity as activity
from flowy.task import SWFActivity as Activity


@activity(79, 'example_list')
class Identity(Activity):

    def run(self, n):
        return n


@activity(79, 'example_list')
class Double(Activity):

    def run(self, n):
        return n * 2


@activity(79, 'example_list')
class Sum(Activity):

    def run(self, *n):
        return sum(n)


@activity(79, 'example_list')
class Square(Activity):

    def run(self, n):
        return n ** 2


@activity(79, 'example_list')
class Error(Activity):

    def run(self, msg='err'):
        raise RuntimeError(msg)


@activity(79, 'example_list', start_to_close=2)
class Timeout(Activity):

    def run(self):
        time.sleep(5)
