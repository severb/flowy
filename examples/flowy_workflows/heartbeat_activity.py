from flowy import make_config, Activity, activity_config
from time import sleep


@activity_config('HeartbeatActivity', 3, 'constant_list', heartbeat=4,
                 schedule_to_close=60, schedule_to_start=60, start_to_close=120)
class HeartbeatActivity(Activity):
    """
    Return constant value

    """
    def run(self, heartbeat):
        for i in range(5):
            heartbeat(i)
            sleep(2)
        return 2


if __name__ == '__main__':
    my_config = make_config(domain='RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='constant_list')
