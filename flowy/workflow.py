import json
from functools import partial
from collections import namedtuple
from contextlib import contextmanager


__all__ = ['Workflow', 'ActivityProxy', 'ActivityError', 'ActivityTimedout']


class Placeholder(object):
    def result(self):
        raise _SyncNeeded()


class Error(object):
    def __init__(self, reason):
        self._reason = reason

    def result(self):
        raise ActivityError('Failed inside activity: %s', self._reason)


class Timeout(object):
    def result(self):
        raise ActivityTimedout('An activity timedout.')


class Result(object):
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class WorkflowExecution(object):
    def __init__(self, decision):
        self._decision = decision
        default = _ActivityOptions(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            task_list=None,
            retry=3
        )
        self._options_stack = [default]

    @contextmanager
    def options(self, heartbeat=None, schedule_to_close=None,
                schedule_to_start=None, start_to_close=None,
                error_handling=None, task_list=None, retry=None):
        if error_handling is not None:
            self._error_handling_stack.append(error_handling)
        options = _ActivityOptions(
            heartbeat,
            schedule_to_close,
            schedule_to_start,
            start_to_close,
            task_list,
            retry
        )
        new_options = self._current_options.update_with(options)
        self._options_stack.append(new_options)
        yield
        self._options_stack.pop()
        if error_handling is not None:
            self._error_handling_stack.pop()

    @property
    def _current_options(self):
        return self._options_stack[-1]

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None, schedule_to_close=None,
                       schedule_to_start=None, start_to_close=None,
                       task_list=None, retry=3):
        activity_options = _ActivityOptions(
            heartbeat,
            schedule_to_close,
            schedule_to_start,
            start_to_close,
            task_list,
            retry
        )
        # Context settings have the highest priority,
        # even higher than the ones sent as arguments in this method!
        options = activity_options.update_with(self._current_options)
        self._decision.queue_activity(
            call_id=call_id,
            name=name,
            version=version,
            input=input,
            heartbeat=options.heartbeat,
            schedule_to_close=options.schedule_to_close,
            schedule_to_start=options.schedule_to_start,
            start_to_close=options.start_to_close,
            task_list=options.task_list,
            context=str(options.retry)
        )


class BoundInstance(object):
    def __init__(self, workflow, workflow_execution):
        self._workflow = workflow
        self._workflow_execution = workflow_execution
        self.options = self._workflow_execution.options

    def __getattr__(self, activity_name):
        activity_proxy = getattr(self._workflow, activity_name)
        if not isinstance(activity_proxy, ActivityProxy):
            raise AttributeError('%s is not an ActivityProxy instance')
        return partial(activity_proxy, self._workflow_execution)


class Workflow(object):
    """ The class that is inherited and needs to implement the activity task
    coordination logic using the :meth:`run` method.

    """
    def run(self, remote, *args, **kwargs):
        """ A subclass must implement the activity task coordination here. """
        raise NotImplemented()

    def __call__(self, input, decision):
        """ Resumes the execution of the workflow.

        Upon resume, the *input* is deserialized with
        :meth:`deserialize_workflow_input`, the :meth:`run` method is executed,
        result is serialized with :meth:`serialize_workflow_result` and
        returned.
        Whenever the workflow is resumed, the call id counter is reset.

        """

        wc = WorkflowExecution(decision)
        decision.dispatch_new_events(wc)

        remote = BoundInstance(self, wc)

        args, kwargs = self.deserialize_workflow_input(input)
        try:
            result = self.run(remote, *args, **kwargs)
        except _SyncNeeded:
            result = None
        except Exception as e:
            decision.fail(e.message)
        else:
            if wc.nothing_running() and wc.nothing_scheduled():
                decision.complete(self.serialize_workflow_result(result))

    @staticmethod
    def deserialize_workflow_input(data):
        """ Deserialize the given workflow input *data*. """
        args_dict = json.loads(data)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_workflow_result(result):
        """ Serialize the given workflow *result*. """
        return json.dumps(result)


class ActivityProxy(object):
    """ The object that represents an activity from the workflows point of
    view.

    Defines an activity with the given *name*, *version* and configuration
    options. The configuration options set here have a higher priority than
    the ones set when registering an activity.
    As far as the workflow that instantiates objects of this type is concerned,
    it has no relevance where these activities are processed, or in what
    manner.

    """
    def __init__(
        self, name, version,
        heartbeat=None,
        schedule_to_close=None,
        schedule_to_start=None,
        start_to_close=None,
        task_list=None,
        retry=3
    ):
        self.name = name
        self.version = version
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.start_to_close = start_to_close
        self.task_list = task_list
        self.retry = retry

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        # Cache the returned proxy for this.f1 is this.f1 to hold.
        proxy_key = (self.name, self.version)
        if proxy_key not in obj._proxy_cache:
            proxy = self._make_proxy(obj)
            obj._proxy_cache[proxy_key] = proxy
        return obj._proxy_cache[proxy_key]

    @staticmethod
    def serialize_activity_input(*args, **kwargs):
        """ Serialize the given activity *args* and *kwargs*. """
        return json.dumps({'args': args, 'kwargs': kwargs})

    @staticmethod
    def deserialize_activity_result(result):
        """ Deserialize the given *result*. """
        return json.loads(result)

    @staticmethod
    def _has_placeholders(args, kwargs):
        a = list(args) + list(kwargs.items())
        return any(
            r._is_placeholder() for r in a if isinstance(r, MaybeResult)
        )

    @staticmethod
    def _get_args_error(args, kwargs):
        a = list(args) + list(kwargs.items())
        for r in filter(lambda x: isinstance(x, MaybeResult), a):
            try:
                r.result()
            except ActivityError as e:
                return e.message

    def _make_proxy(self, workflow):

        def proxy(*args, **kwargs):
            call_id = workflow._next_call_id()
            context = workflow._context

            if context.is_activity_timedout(call_id):
                if context.should_retry(call_id):
                    input = self.serialize_activity_input(*args, **kwargs)
                    workflow._queue_activity(
                        call_id,
                        self.name,
                        self.version,
                        input,
                        self.heartbeat,
                        self.schedule_to_close,
                        self.schedule_to_start,
                        self.start_to_close,
                        self.task_list,
                        self.retry
                    )
                    return MaybeResult()
                else:
                    if workflow._manual_exception_handling:
                        return MaybeResult(ActivityTimedout(), is_error=True)
                    else:
                        raise _UnhandledActivityError("Activity timed out.")

            sentinel = object()
            result = context.activity_result(call_id, sentinel)
            error_msg = context.activity_error(call_id, sentinel)

            if result is sentinel and error_msg is sentinel:
                args_error = self._get_args_error(args, kwargs)
                if args_error:
                    raise _UnhandledActivityError(
                        'Error when calling activity: %s' % args_error
                    )
                placeholders = self._has_placeholders(args, kwargs)
                scheduled = context.is_activity_running(call_id)
                if not placeholders and not scheduled:
                    input = self.serialize_activity_input(*args, **kwargs)
                    workflow._queue_activity(
                        call_id,
                        self.name,
                        self.version,
                        input,
                        self.heartbeat,
                        self.schedule_to_close,
                        self.schedule_to_start,
                        self.start_to_close,
                        self.task_list,
                        self.retry
                    )
                return MaybeResult()
            if error_msg is not sentinel:
                if workflow._manual_exception_handling:
                    return MaybeResult(ActivityError(error_msg), is_error=True)
                else:
                    raise _UnhandledActivityError(error_msg)
            return MaybeResult(self.deserialize_activity_result(result))

        return proxy


_AOBase = namedtuple(
    typename='_ActivityOptions',
    field_names=[
        'heartbeat',
        'schedule_to_close',
        'schedule_to_start',
        'start_to_close',
        'task_list',
        'retry'
    ]
)


class _ActivityOptions(_AOBase):
    def update_with(self, other):
        t_pairs = zip(other, self)
        updated_fields = [x if x is not None else y for x, y in t_pairs]
        return _ActivityOptions(*updated_fields)


class _SyncNeeded(Exception):
    """Stops the workflow execution when an activity result is unavailable."""


class ActivityError(RuntimeError):
    """Raised if manual handling is ON if there is a problem in an activity."""


class ActivityTimedout(ActivityError):
    """Raised if manual handling is ON on activity timeout."""
