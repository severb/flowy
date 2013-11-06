from flowy import make_config, Activity, activity_config


@activity_config(
    'NumberDivider', 4, 'div_list', heartbeat=5, start_to_close=60
)
class NumberDivider(Activity):
    """
    Divide numbers.

    """
    def run(self, heartbeat, n, x):
        import time

        for i in range(3):
            time.sleep(3)
            if not heartbeat():
                print 'abort abort!'
                return
#         time.sleep(10)
#         if not self.heartbeat():
#             print 'abort abort'
#             return
        return n % x == 0


if __name__ == '__main__':
    my_config = make_config(domain='SeversTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='div_list')
