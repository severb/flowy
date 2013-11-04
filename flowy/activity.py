import json
from contextlib import contextmanager


class Activity(object):
    """ A simple baseclass for activities that removes some boilerplate.

    A subclass is required to implement :meth:`run`. This class provides
    argument deserialization, result serialization and error handling
    automatically. An activity can finish with error by raising an exception
    from inside the ``run`` method and its ``message`` property will be used as
    the reason of the failure.

    By default this baseclass uses ``JSON`` for activity arguments and result
    transport. This can be changed by overriding
    :meth:`deserialize_activity_input` and :meth:`serialize_activity_result`
    methods.

    """

    def run(self, *args, **kwargs):
        """ The actual unit of work must be implemented here. """
        raise NotImplemented()

    def __call__(self, input, activity_task):
        """ The actual entrypoint for the activity.

        Here we deserialize the input call the activity implementation and
        serialize the result.

        """
        args, kwargs = self.deserialize_activity_input(input)
        try:
            with self._bind_to(activity_task):
                result = self.run(*args, **kwargs)
        except Exception as e:
            activity_task.fail(e.message)
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

    def heartbeat(self):
        """ Use the heartbeat to send regular updates from activities.

        If the heartbeat returns a false value the activity should be aborted
        after an optional cleanup since one of its timeout counters was
        exceeded.

        """
        raise RuntimeError('The heartbeat is unbound.')

    @contextmanager
    def _bind_to(self, activity_task):
        old_heartbeat = self.heartbeat
        self.heartbeat = activity_task.heartbeat
        yield
        self.heartbeat = old_heartbeat
