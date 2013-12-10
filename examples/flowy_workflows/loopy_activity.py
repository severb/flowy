from flowy import make_config, Activity, activity_config


@activity_config('RangeActivity', 2, 'a_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class RangeActivity(Activity):
    """
    Return constant value

    """
    def run(self, heartbeat):
        return 3


@activity_config('OperationActivity', 2, 'a_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class OperationActivity(Activity):
    """
    Return double value of parameter

    """
    def run(self, heartbeat, n):
        return 2*n


if __name__ == '__main__':
    my_config = make_config(domain='RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='a_list')
