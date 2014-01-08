import json

__all__ = ['Activity']


class Activity(object):
    """ A simple base class for activities that removes some boilerplate.

    A subclass is required to implement :meth:`run`. This class provides
    argument deserialization, result serialization and error handling
    automatically. An activity can finish successfully by returning a value and
    can fail by having an unhandled exception inside the `run` method.

    By default this baseclass uses ``JSON`` for activity arguments and result
    transport. This can be changed by overriding
    :meth:`deserialize_activity_input` and :meth:`serialize_activity_result`
    methods.

    """

    def run(self, heartbeat, *args, **kwargs):
        """ The actual unit of work must be implemented here.

        Gets a *heartbeat* callable as the first argument that can be used to
        send regular updates from the activity. If the heartbeat returns a
        `False` value the activity should be aborted after an optional cleanup
        since one of its timeout counters was exceeded.

        """
        raise NotImplementedError()

    def __call__(self, input, activity_task):
        args, kwargs = self.deserialize_activity_input(input)
        try:
            result = self.run(activity_task.heartbeat, *args, **kwargs)
        except Exception as e:
            activity_task.fail(str(e))
        else:
            activity_task.complete(self.serialize_activity_result(result))

    @staticmethod
    def deserialize_activity_input(input):
        """ Deserialize the activity *input*. """
        args_dict = json.loads(input)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_activity_result(result):
        """ Serialize the given *result*. """
        return json.dumps(result)
