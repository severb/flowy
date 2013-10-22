import json


class Activity(object):
    """ The base class for an activity that must implement the :meth:`run`
    method.

    """
    def run(self, *args, **kwargs):
        """ The actual unit of work must be implemented here. """
        raise NotImplemented()

    def call(self, input, client):
        """ Call the activity with the given *input* and bind the
        :class:`flowy.client.SWFClient` *client* to this instance for the
        duration of the call so that heartbeats can be sent from the activity.

        """
        self._client = client
        args, kwargs = self.deserialize_activity_input(input)
        result = self.serialize_activity_result(self.run(*args, **kwargs))
        self._client = None
        return result

    @staticmethod
    def deserialize_activity_input(input):
        """ Deserialize the activity *input*. """
        args_dict = json.loads(input)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_activity_result(result):
        """ Serialize the given *result*. """
        return json.dumps(result)

    def heartbeat(self):
        """ Signal that the activity is making progress. """
        return self._client.heartbeat()
