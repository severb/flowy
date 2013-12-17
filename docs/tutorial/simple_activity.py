from flowy import Activity, make_config, activity_config


@activity_config('EvenChecker', 1, 'math_list', heartbeat=5, start_to_close=60)
class EvenChecker(Activity):
    """ Check if the given number is even. """

    def run(self, heartbeat, n):
        return not bool(n & 1)


if __name__ == '__main__':
    my_config = make_config('RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='math_list')
