@activity_config('EvenChecker', 1, 'math_list', heartbeat=5,
                 schedule_to_close=60, schedule_to_start=120,
                 start_to_close=300, descr="Even")
class EvenChecker(Activity):
    """ Check if the given number is even. """

    def run(self, heartbeat, n):
        return not bool(n & 1)

@workflow_config('MyParity', 1, 'math_list')
class ParityTest(Workflow):
    """ Checks if a number is even or odd. """

    even = ActivityProxy(name='EvenChecker', version=1, task_list='math_list',
                         heartbeat=5, start_to_close=60, schedule_to_start=100,
                         schedule_to_close=50, retry=5, delay=0)

    def run(self, remote, n=77):
        with remote.options(delay=10, retry=2, heartbeat=10, start_to_close=10,
                            task_start_to_close=100, task_list='other_list',
                            execution_start_to_close=150):
            r = remote.even(n)
        if r.result():
            return 'even'
        return 'odd'


