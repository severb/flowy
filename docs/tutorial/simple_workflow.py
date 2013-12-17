from flowy import Workflow, make_config, workflow_config, ActivityProxy


@workflow_config('MyParity', 1, 'math_list')
class ParityTest(Workflow):
    """ Checks if a number is even or odd. """

    even = ActivityProxy(name='EvenChecker', version=1, task_list='math_list',
                         heartbeat=5, start_to_close=60)

    def run(self, remote, n=77):
        r = remote.even(n)
        if r.result():
            return 'even'
        return 'odd'


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    ParityWF = my_config.workflow_starter('MyParity', 1)
    print 'Starting: ', ParityWF(40)

    my_config.scan()
    my_config.start_workflow_loop(task_list='math_list')
