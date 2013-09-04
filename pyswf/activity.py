class ActivityError(RuntimeError):
    pass


class activity(object):
    def __init__(self, name, version):
        self.name = name
        self.version = str(version)

    def __call__(self, activity):
        self.activity = activity
        self.__call__ = self.run_activity
        return self

    def run_activity(self, *args, **kwargs):
        try:
            return self.activity(*args, **kwargs)
        except Exception as e:
            raise ActivityError(e.message)
