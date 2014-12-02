class SingleThreadedWorker(object):
    def __init__(self, poller):
        self._poller = poller

    def run_forever(self):
        for task in self._poller:
            task()
