import time

from flowy.swf.scanner import activity
from flowy.task import Activity


@activity('Identity', 77, 'example_list')
class Identity(Activity):

    def run(self, n):
        return n


@activity('Double', 77, 'example_list')
class Double(Activity):

    def run(self, n):
        return n * 2


@activity('Sum', 77, 'example_list')
class Sum(Activity):

    def run(self, *n):
        return sum(n)


@activity('Square', 77, 'example_list')
class Square(Activity):

    def run(self, n):
        return n ** 2


@activity('Error', 77, 'example_list')
class Error(Activity):

    def run(self, msg='err'):
        raise RuntimeError(msg)


@activity('Timeout', 77, 'example_list', start_to_close=2)
class Timeout(Activity):

    def run(self):
        time.sleep(5)
