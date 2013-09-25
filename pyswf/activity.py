import json


class ActivityError(RuntimeError):
    pass


class ActivityTimedout(RuntimeError):
    pass


class Activity(object):
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
