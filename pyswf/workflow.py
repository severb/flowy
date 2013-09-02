import json
from boto.swf.layer1_decisions import Layer1Decisions
from boto.swf.layer1 import Layer1


class SyncNeeded(Exception):
    pass


class ActivityError(RuntimeError):
    pass


class History(object):

    input_as_args = []
    input_as_kwargs = {}

    def __init__(self, history):
        self.history = history

    @property
    def task_token(self):
        return self.history['taskToken']

    @property
    def workflow_name(self):
        return self.history['workflowType']['name']

    @property
    def workflow_version(self):
        return self.history['workflowType']['version']

    @property
    def events(self):
        return self.history['events']

    @property
    def scheduled_activities(self):
        return filter(
            lambda e: e['eventType'] == 'ActivityTaskScheduled',
            self.events
        )

    @property
    def completed_activities(self):
        return filter(
            lambda e: e['eventType'] == 'ActivityTaskCompleted',
            self.events
        )

    def is_scheduled(self, invocation_id):
        ATSEA = 'activityTaskScheduledEventAttributes'
        for event in self.scheduled_activities:
            if event[ATSEA]['activityId'] == invocation_id:
                return event
        return False

    def result_for(self, invocation_id, default=None):
        schedule = self.is_scheduled(invocation_id)
        if not schedule:
            return default
        event_id = schedule['eventId']
        ATCEA = 'activityTaskCompletedEventAttributes'
        for event in self.completed_activities:
            if event[ATCEA]['scheduledEventId'] == event_id:
                return MaybeResult(
                    self.deserialize_input(event[ATCEA]['result'])
                )
        return default

    def deserialize_input(self, input):
        return int(input[4:6])
        return json.loads(input)


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
    def __init__(self, history):
        self._history = history
        self._current_invocation = 0
        self._scheduled = set()
        self._proxy_cache = dict()

    def _next_invocation_id(self):
        result = self._current_invocation
        self._current_invocation += 1
        return str(result)

    def _is_scheduled(self, invocation_id):
        return self._history.is_scheduled(invocation_id)

    def _result_for(self, invocation_id, default=None):
        return self._history.result_for(invocation_id, default)

    def _schedule(self, invocation_id, activity, input):
        self._scheduled.add((invocation_id, activity, input))

    def __call__(self, *args, **kwargs):
        try:
            self.run()
        except SyncNeeded:
            pass

    def run(self, *args, **kwargs):
        raise NotImplemented()


class Activity(object):
    def __init__(self, name, version):
        self.name = name
        self.version = version

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if (self.name, self.version) not in obj._proxy_cache:
            def proxy(*args, **kwargs):
                invocation_id = obj._next_invocation_id()
                result = obj._result_for(invocation_id, MaybeResult())
                if result._is_placeholder():
                    if (self.no_placeholders(args, kwargs)
                        and not obj._is_scheduled(invocation_id)
                    ):
                        input = self.serialize_input(args, kwargs)
                        obj._schedule(invocation_id, self, input)
                return result
            obj._proxy_cache[(self.name, self.version)] = proxy
        return obj._proxy_cache[(self.name, self.version)]

    def no_placeholders(self, args, kwargs):
        a = list(args) + list(kwargs.items())
        return all(
            not r._is_placeholder() for r in a if isinstance(r, MaybeResult)
        )

    def serialize_input(self, args, kwargs):
        args = [
            isinstance(arg, MaybeResult) and arg.result() or arg
            for arg in args
        ]
        kwargs = dict(
            (k, isinstance(v, MaybeResult) and v.result() or v)
            for k, v in kwargs.items()
        )
        return json.dumps({
            'args': args,
            'kwargs': kwargs
        })
