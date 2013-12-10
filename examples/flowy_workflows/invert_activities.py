from flowy import make_config, Activity, activity_config


@activity_config('NumberDivider', 2, 'invert_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, heartbeat, n, x):
        print(n,x)
        return n % x == 1


if __name__ == '__main__':
    my_config = make_config(domain='RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='invert_list')
