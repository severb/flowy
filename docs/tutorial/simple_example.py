from flowy.activity import Activity
from flowy.workflow import Workflow, ActivityProxy
from flowy.client import workflow_client, activity_client


@workflow_client('MyParity', 2)
class ParityTest(Workflow):
    """ Checks if a number is even or odd. """

    even = ActivityProxy(name='EvenChecker', version=4, task_list='math_list',
                         heartbeat=5, start_to_close=60)

    def run(self, n=77):
        r = self.even(n)
        if r.result():
            return 'even'
        return 'odd'


@activity_client('EvenChecker', 4, heartbeat=5, start_to_close=60)
class EvenChecker(Activity):
    """ Check if the given number is even. """

    def run(self, n):
        return not bool(n & 1)


activity_client.start_on('SimpleExample', 'math_task_list')
workflow_client.start_on('SimpleExample', 'math_list')
