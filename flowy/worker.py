class SingleThreadedWorker(object):
    def __init__(self, client):
        self._client = client
        self._registry = {}

    def register(self, name, version, task_factory):
        self._registry[(name, version)] = task_factory

    def poll_next_task(self):
        return self._client.poll_next_task(self)

    def make_task(self, name, version, input, result, runtime):
        task_factory = self._registry.get((name, version))
        if task_factory is not None:
            return task_factory(input=input, result=result, runtime=runtime)
        return None

    def run_forever(self):
        while 1:
            task = self.poll_next_task()
            task()
