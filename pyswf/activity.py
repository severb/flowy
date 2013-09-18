import json


class ActivityError(RuntimeError):
    pass


class ActivityTimedout(RuntimeError):
    pass


class Activity(object):

    schedule_to_close = 300
    schedule_to_start = 60
    heartbeat = 30
    task_start_to_close = 120
    name = None
    version = None

    def run(self, *args, **kwargs):
        raise NotImplemented()

    def call(self, input):
        args, kwargs = self.deserialize_activity_input(input)
        return self.serialize_activity_result(self.run(*args, **kwargs))

    @staticmethod
    def deserialize_activity_input(input):
        args_dict = json.loads(input)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_activity_result(result):
        return json.dumps(result)
