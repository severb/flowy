class ActivityError(RuntimeError):
    pass


class activity(object):
    def __init__(self, name, version):
        self.name = name
        self.version = version

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
