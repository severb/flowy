class SingleThreadedWorker(object):
    def __init__(self, poller):
        self._poller = poller

    def run_forever(self, loop=-1):
        while loop:
            task = self._poller.poll_next_task()
            task()
            loop = max(-1, loop - 1)
