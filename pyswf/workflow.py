from pyswf.activity import ActivityError, ActivityTimedout

class SyncNeeded(Exception):
    pass


class MaybeResult(object):

    sentinel = object()

    def __init__(self, result=sentinel):
        self.r = result

    def result(self):
        if self._is_placeholder():
            raise SyncNeeded()
        return self.r

    def _is_placeholder(self):
        return self.r is self.sentinel


class Workflow(object):

    child_policy = 'TERMINATE'
    execution_start_to_close = 3600
    task_start_to_close = 60

    def __init__(self, execution_context):
        self._execution_context = execution_context
        self._current_invocation = 0
        self._scheduled = []
        self._proxy_cache = dict()

    def _next_invocation_id(self):
        result = self._current_invocation
        self._current_invocation += 1
        return str(result)

    def _is_scheduled(self, invocation_id):
        return self._execution_context.is_scheduled(invocation_id)

    def _result_for(self, invocation_id, default=None):
        return self._execution_context.result_for(invocation_id, default)

    def _is_error(self, invocation_id):
        return self._execution_context.is_error(invocation_id)

    def _is_timedout(self, invocation_id):
        return self._execution_context.is_timedout(invocation_id)

    def _schedule(self, invocation_id, activity, args, kwargs):
        self._scheduled.append((invocation_id, activity, args, kwargs))

    def invoke(self, *args, **kwargs):
        result = None
        try:
            result = self.run(*args, **kwargs)
        except SyncNeeded:
            pass
        return self._scheduled, result

    def run(self, *args, **kwargs):
        raise NotImplemented()


class ActivityProxy(object):
    _sentinel = object()

    def __init__(self, name, version):
        self.name = name
        self.version = version

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        # We need to cache the returned proxy for this.f1 is this.f1 to hold
        if (self.name, self.version) not in obj._proxy_cache:
            def proxy(*args, **kwargs):
                invocation_id = obj._next_invocation_id()
                if obj._is_timedout(invocation_id):
                    raise ActivityTimedout()
                result = obj._result_for(invocation_id, self._sentinel)
                if result is self._sentinel:
                    if (self.no_placeholders(args, kwargs)
                        and not obj._is_scheduled(invocation_id)
                    ):
                        obj._schedule(invocation_id, self, args, kwargs)
                    return MaybeResult()
                if obj._is_error(invocation_id):
                    raise ActivityError(result)
                return MaybeResult(result)
            obj._proxy_cache[(self.name, self.version)] = proxy
        return obj._proxy_cache[(self.name, self.version)]

    @staticmethod
    def no_placeholders(args, kwargs):
        a = list(args) + list(kwargs.items())
        return all(
            not r._is_placeholder() for r in a if isinstance(r, MaybeResult)
        )
