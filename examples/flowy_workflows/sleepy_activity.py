from flowy import make_config, Activity, activity_config
from time import sleep


@activity_config('SleepyActivity', 1, 'constant_list', heartbeat=60,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class SleepyActivity(Activity):
    """
    Return constant value

    """
    def run(self, heartbeat, i):
        sleep(i)
        return 2


if __name__ == '__main__':
    my_config = make_config(domain='RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='constant_list')
