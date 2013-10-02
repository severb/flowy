import json
from collections import namedtuple
from contextlib import contextmanager


class _SyncNeeded(Exception):
    pass


class _UnhandledActivityError(Exception):
    pass


class ActivityError(RuntimeError):
    pass


class ActivityTimedout(ActivityError):
    pass


ActivityCall = namedtuple('ActivityCall', 'call_id name version input options')


class ActivityOptions(
    namedtuple(
        'ActivityOptionsBase',
        'heartbeat schedule_to_close schedule_to_start start_to_close'
    )
):
    def update_with(self, other):
        return ActivityOptions(
            other.heartbeat if other.heartbeat is not None
                else self.heartbeat,
            other.schedule_to_close if other.schedule_to_close is not None
                else self.schedule_to_close,
            other.schedule_to_start if other.schedule_to_start is not None
                else self.schedule_to_start,
            other.start_to_close if other.start_to_close is not None
                else self.start_to_close,
        )


class MaybeResult(object):

    sentinel = object()

    def __init__(self, result=sentinel, is_error=False):
        self.r = result
        self._is_error = is_error

    def result(self):
        if self.is_placeholder():
            raise _SyncNeeded()
        if self._is_error:
            raise ActivityError(self.r)
        return self.r

    def is_placeholder(self):
        return self.r is self.sentinel


class Workflow(object):
    def __init__(self):
        self._current_call_id = 0
        self._proxy_cache = dict()
        self._options_stack = [ActivityOptions(None, None, None, None)]
        self._error_handling_stack = [False]

    @contextmanager
    def options(self,
        heartbeat=None, schedule_to_close=None,
        schedule_to_start=None, start_to_close=None, error_handling=None
    ):
        if error_handling is not None:
            self._error_handling_stack.append(error_handling)
        options = ActivityOptions(
            heartbeat, schedule_to_close, schedule_to_start, start_to_close)
        new_options = self._current_options.update_with(options)
        self._options_stack.append(new_options)
        yield
        self._options_stack.pop()
        if error_handling is not None:
            self._error_handling_stack.pop()

    @property
    def _current_options(self):
        return self._options_stack[-1]

    @property
    def _manual_exception_handling(self):
        return self._error_handling_stack[-1]

    def _next_call_id(self):
        result = self._current_call_id
        self._current_call_id += 1
        return result

    def _queue_activity(self, call_id, name, version, input,
        heartbeat=None, schedule_to_close=None,
        schedule_to_start=None, start_to_close=None
    ):
        activity_options = ActivityOptions(
            heartbeat, schedule_to_close,
            schedule_to_start, start_to_close
        )
        options = activity_options.update_with(self._current_options)
        self._scheduled.append(
            ActivityCall(call_id, name, version, input, options)
        )

    def resume(self, input, context):
        self._context, result = context, None
        self._scheduled = []
        args, kwargs = self.deserialize_workflow_input(input)
        try:
            result = self.run(*args, **kwargs)
        except _SyncNeeded:
            pass
        self._current_call_id = 0
        return self.serialize_workflow_result(result), self._scheduled

    def run(self, *args, **kwargs):
        raise NotImplemented()

    @staticmethod
    def deserialize_workflow_input(data):
        args_dict = json.loads(data)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_workflow_result(result):
        return json.dumps(result)


class ActivityProxy(object):
    def __init__(self, name, version, heartbeat=None, schedule_to_close=None,
        schedule_to_start=None, start_to_close=None
    ):
        self.name = name
        self.version = version
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.start_to_close = start_to_close

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        # Cache the returned proxy for this.f1 is this.f1 to hold.
        proxy_key = (self.name, self.version)
        if proxy_key not in obj._proxy_cache:
            proxy = self.make_proxy(obj)
            obj._proxy_cache[proxy_key] = proxy
        return obj._proxy_cache[proxy_key]

    @staticmethod
    def serialize_activity_input(*args, **kwargs):
        return json.dumps({'args': args, 'kwargs': kwargs})

    @staticmethod
    def deserialize_activity_result(result):
        return json.loads(result)

    @staticmethod
    def has_placeholders(args, kwargs):
        a = list(args) + list(kwargs.items())
        return any(
            r.is_placeholder() for r in a if isinstance(r, MaybeResult)
        )

    @staticmethod
    def get_args_error(args, kwargs):
        a = list(args) + list(kwargs.items())
        for r in filter(lambda x: isinstance(x, MaybeResult), a):
            try:
                r.result()
            except ActivityError as e:
                return e.message

    def make_proxy(self, workflow):

        def proxy(*args, **kwargs):
            call_id = workflow._next_call_id()
            context = workflow._context

#             if context.is_activity_timedout(call_id):
                # Reschedule if needed
                # return MaybeResult
                # raise ActivityTimedout
#                 pass

            sentinel = object()
            result = context.activity_result(call_id, sentinel)
            error = context.activity_error(call_id, sentinel)

            if result is sentinel and error is sentinel:
                args_error = self.get_args_error(args, kwargs)
                if args_error:
                    raise _UnhandledActivityError(
                        'Error when calling activity: %s' % args_error
                    )
                placeholders = self.has_placeholders(args, kwargs)
                scheduled = context.is_activity_scheduled(call_id)
                if not placeholders and not scheduled:
                    input = self.serialize_activity_input(*args, **kwargs)
                    workflow._queue_activity(
                        call_id, self.name, self.version, input,
                        self.heartbeat,
                        self.schedule_to_close,
                        self.schedule_to_start,
                        self.start_to_close
                    )
                return MaybeResult()
            if error is not sentinel:
                if workflow._manual_exception_handling:
                    return MaybeResult(error, is_error=True)
                else:
                    raise _UnhandledActivityError(error)
            return MaybeResult(self.deserialize_activity_result(result))

        return proxy
