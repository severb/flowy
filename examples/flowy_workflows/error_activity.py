from flowy import make_config, Activity, activity_config


@activity_config('ErrorActivity', 1, 'constant_list', schedule_to_close=5,
                 schedule_to_start=5, start_to_close=10)
class ErrorActivity(Activity):
    """
    Return constant value

    """
    def run(self, heartbeat, raise_error):
        print(raise_error)
        if raise_error:
            raise Exception("Erorr")
        else:
            from time import sleep
            sleep(15)


if __name__ == '__main__':
    my_config = make_config(domain='RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='constant_list')
