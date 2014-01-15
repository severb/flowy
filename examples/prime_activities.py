from flowy import make_config, Activity, activity_config


@activity_config(
    'NumberDivider', 4, 'div_list', heartbeat=5, start_to_close=60
)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, heartbeat, n, x):
        return n % x == 0


if __name__ == '__main__':
    my_config = make_config(domain='SeversTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='div_list')
