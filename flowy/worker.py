class SingleThreadedWorker(object):
    def __init__(self, poller):
        self._poller = poller

    def run_forever(self, loop=-1):
        loop = int(loop)
        for task in self._poller:
            loop = max(-1, loop - 1)
            if loop == 0:
                break
            task()
