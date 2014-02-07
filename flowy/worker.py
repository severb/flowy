class SingleThreadedWorker(object):
    def __init__(self, client):
        self._client = client
        self._registry = {}

    def register(self, task_id, task_factory):
        self._registry[task_id] = task_factory

    def poll_next_task(self):
        return self._client.poll_next_task(self)

    def make_task(self, task_id, input, scheduler):
        task_factory = self._registry.get(task_id)
        if task_factory is not None:
            return task_factory(input=input, scheduler=scheduler)
        return None

    def run_forever(self, loop=-1):
        while loop != 0:
            task = self.poll_next_task()
            task()
            loop = max(-1, loop - 1)
