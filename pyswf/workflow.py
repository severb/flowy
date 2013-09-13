import json
from contextlib import contextmanager

from pyswf.activity import ActivityError, ActivityTimedout


class _SyncNeeded(Exception):
    pass


class _UnhandledActivityError(Exception):
    pass


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

    child_policy = 'TERMINATE'
    execution_start_to_close = 3600
    task_start_to_close = 60

    def __init__(self, context, response):
        self._context = context
        self._response = response
        self._current_call_id = 0
        self._proxy_cache = dict()
        self.error_handling_nesting_level = 0

    @contextmanager
    def error_handling(self):
        self.error_handling_nesting_level += 1
        yield
        self.error_handling_nesting_level -= 1

    @property
    def manual_exception_handling(self):
        return self.error_handling_nesting_level > 0

    def _next_call_id(self):
        result = self._current_call_id
        self._current_call_id += 1
        return str(result)

    def resume(self):
        result = None
        args, kwargs = self.deserialize_workflow_input(self._context.input)
        try:
            result = self.run(*args, **kwargs)
        except _SyncNeeded:
            pass
        return self.serialize_workflow_result(result)

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
    def __init__(self, name, version):
        self.name = name
        self.version = version

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
            response = workflow._response

#             if context.is_activity_timedout(call_id):
                # Reschedule if needed
                # return MaybeResult
                # raise ActivityTimedout
#                 pass

            _sentinel = object()
            result = context.activity_result(call_id, _sentinel)
            error = context.activity_error(call_id, _sentinel)

            if result is _sentinel and error is _sentinel:
                args_error = self.get_args_error(args, kwargs)
                if args_error:
                    raise _UnhandledActivityError(
                        'Error when calling activity: %s' % args_error
                    )
                placeholders = self.has_placeholders(args, kwargs)
                scheduled = context.is_activity_scheduled(call_id)
                if not placeholders and not scheduled:
                    input = self.serialize_activity_input(*args, **kwargs)
                    workflow._response.schedule(
                        call_id, self.name, self.version, input
                    )
                return MaybeResult()
            if error is not _sentinel:
                if workflow.manual_exception_handling:
                    return MaybeResult(error, is_error=True)
                else:
                    raise _UnhandledActivityError(error)
            return MaybeResult(self.deserialize_activity_result(result))

        return proxy
