import json


class Activity(object):
    def run(self, *args, **kwargs):
        raise NotImplemented()

    def call(self, input, client):
        self._client = client
        args, kwargs = self.deserialize_activity_input(input)
        result = self.serialize_activity_result(self.run(*args, **kwargs))
        self._client = None
        return result

    @staticmethod
    def deserialize_activity_input(input):
        args_dict = json.loads(input)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_activity_result(result):
        return json.dumps(result)

    def heartbeat(self):
        self._client.heartbeat()
