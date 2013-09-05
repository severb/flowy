class ActivityError(RuntimeError):
    pass


class activity(object):
    def __init__(self, name, version,
        schedule_to_close=300, schedule_to_start=60,
        heartbeat=30, task_start_to_close=120
    ):
        self.name = name
        self.version = version
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.task_start_to_close = task_start_to_close

    @property
    def id(self):
        return self.name, self.version

    def __call__(self, activity):
        self.activity = activity
        return self

    def invoke(self, *args, **kwargs):
        try:
            return self.activity(*args, **kwargs)
        except Exception as e:
            raise ActivityError(e.message)
