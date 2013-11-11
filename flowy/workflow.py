import json
from functools import partial
from collections import namedtuple
from contextlib import contextmanager


__all__ = 'Workflow ActivityProxy WorkflowProxy TaskError TaskTimedout'.split()


class Placeholder(object):
    def result(self):
        raise _SyncNeeded()


class Error(object):
    def __init__(self, reason):
        self._reason = reason

    def result(self):
        raise TaskError('Failed inside job: %s', self._reason)


class Timeout(object):
    def result(self):
        raise TaskTimedout('A job timedout.')


class Result(object):
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class JSONExecutionHistory(object):
    def __init__(self, context=None):
        self._running = set()
        self._timedout = set()
        self._results = {}
        self._errors = {}
        if context is not None:
            json_ctx = json.loads(context)
            running, timedout, self._results, self._errors = json_ctx
            self._running = set(running)
            self._timedout = set(timedout)

    def activity_scheduled(self, call_id):
        if call_id in self._timedout:
            self._timedout.remove(call_id)
        self._running.add(call_id)

    def activity_completed(self, call_id, result):
        self._running.remove(call_id)
        self._results[call_id] = result

    def activity_failed(self, call_id, reason):
        self._running.remove(call_id)
        self._errors[call_id] = reason

    def activity_timedout(self, call_id):
        self._running.remove(call_id)
        self._timedout.add(call_id)

    workflow_started = activity_scheduled
    workflow_completed = activity_completed
    workflow_failed = activity_failed
    workflow_timedout = activity_timedout

    def any_activity_running(self):
        return bool(self._running)

    def call_running(self, call_id):
        return call_id in self._running

    def call_timedout(self, call_id):
        return call_id in self._timedout

    def call_error(self, call_id, default=None):
        return self._errors.get(call_id, default)

    def call_result(self, call_id, default=None):
        return self._results.get(call_id, default)

    def serialize(self):
        data = (
            list(self._running),
            list(self._timedout),
            self._results,
            self._errors
        )
        return json.dumps(data)


class WorkflowExecution(object):
    def __init__(self, decision, execution_history):

        self._decision = decision
        self._exec_history = execution_history

        default = _Options(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            task_start_to_close=None,
            execution_start_to_close=None,
            task_list=None,
            retry=3
        )
        self._options_stack = [default]
        self._error_handling_stack = [False]
        self._call_id = '0'
        self._failed = False
        self._queued = False

    @contextmanager
    def options(self, heartbeat=None, schedule_to_close=None,
                schedule_to_start=None, start_to_close=None,
                error_handling=None, task_start_to_close=None,
                execution_start_to_close=None, task_list=None, retry=None):
        if error_handling is not None:
            self._error_handling_stack.append(error_handling)
        options = _Options(
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_start_to_close=task_start_to_close,
            execution_start_to_close=execution_start_to_close,
            task_list=task_list,
            retry=retry
        )
        new_options = self._current_options.update_with(options)
        self._options_stack.append(new_options)
        yield
        self._options_stack.pop()
        if error_handling is not None:
            self._error_handling_stack.pop()

    def queue_activity(self, name, version, input,
                       heartbeat=None, schedule_to_close=None,
                       schedule_to_start=None, start_to_close=None,
                       task_list=None, retry=3):
        self._queued = True
        activity_options = _Options(
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_start_to_close=None,
            execution_start_to_close=None,
            task_list=task_list,
            retry=retry
        )
        # Context settings have the highest priority,
        # even higher than the ones sent as arguments in this method!
        options = activity_options.update_with(self._current_options)
        retry = options.retry
        prev_retry = self._decision.activity_context(self._call_id)
        if prev_retry is not None:
            retry = int(prev_retry) - 1
        return self._decision.queue_activity(
            call_id=self._call_id,
            name=name,
            version=version,
            input=input,
            heartbeat=options.heartbeat,
            schedule_to_close=options.schedule_to_close,
            schedule_to_start=options.schedule_to_start,
            start_to_close=options.start_to_close,
            task_list=options.task_list,
            context=str(retry)
        )

    def queue_childworkflow(self, name, version, input,
                            task_start_to_close=None,
                            execution_start_to_close=None,
                            task_list=None, retry=3):
        self._queued = True
        workflow_options = _Options(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            task_start_to_close=task_start_to_close,
            execution_start_to_close=execution_start_to_close,
            task_list=task_list,
            retry=retry
        )
        options = workflow_options.update_with(self._current_options)
        retry = options.retry
        prev_retry = self._decision.workflow_context(self._call_id)
        if prev_retry is not None:
            retry = int(prev_retry) - 1
        return self._decision.queue_childworkflow(
            call_id=self._call_id,
            name=name,
            version=version,
            input=input,
            task_start_to_close=options.task_start_to_close,
            execution_start_to_close=options.execution_start_to_close,
            task_list=options.task_list,
            context=str(retry)
        )

    def fail(self, reason):
        # Potentially this can be called multiple times one one run (i.e. an
        # activity that was used as argument for other two activities failed)
        if not self._failed:
            self._failed = True
            self._decision.fail(reason)

    def has_failed(self):
        return self._failed

    def any_activity_running(self):
        return self._exec_history.any_activity_running()

    def any_activity_queued(self):
        return self._queued

    def next_call(self):
        self._call_id = str(int(self._call_id) + 1)

    def current_call_running(self):
        return self._exec_history.call_running(self._call_id)

    def current_call_timedout(self):
        return self._exec_history.call_timedout(self._call_id)

    def current_call_error(self):
        return self._exec_history.call_error(self._call_id)

    def current_call_result(self):
        return self._exec_history.call_result(self._call_id)

    def should_retry(self):
        return int(self._decision.activity_context(self._call_id, 0)) > 0

    def error_handling(self):
        return bool(self._error_handling_stack[-1])

    @property
    def _current_options(self):
        return self._options_stack[-1]


class BoundInstance(object):
    def __init__(self, workflow, workflow_execution):
        self._workflow = workflow
        self._workflow_execution = workflow_execution
        self.options = self._workflow_execution.options

    def __getattr__(self, task_name):
        task_proxy = getattr(self._workflow, task_name)
        if not isinstance(task_proxy, BaseProxy):
            raise AttributeError('%s is not a Proxy instance' % task_name)
        return partial(task_proxy, self._workflow_execution)


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

        """

        execution_history = JSONExecutionHistory(decision.global_context())
        decision.dispatch_new_events(execution_history)
        wc = WorkflowExecution(decision, execution_history)

        remote = BoundInstance(self, wc)

        args, kwargs = self.deserialize_workflow_input(input)
        try:
            result = self.run(remote, *args, **kwargs)
        except _SyncNeeded:
            # It's ok to pass here since the code that follows won't mistakenly
            # complete the workflow - nothing_running should always return
            # False as long as there is at least an activity running: the one
            # that blocked us in the first place.
            pass
        except Exception as e:
            decision.fail(e.message)
            return

        # If the execution failed return early to avoid attempting to schedule
        # activities on a failed workflow.
        if wc.has_failed():
            return

        if not wc.any_activity_running() and not wc.any_activity_queued():
            decision.complete(self.serialize_workflow_result(result))
            return

        decision.schedule_queued(execution_history.serialize())

    @staticmethod
    def deserialize_workflow_input(data):
        """ Deserialize the given workflow input *data*. """
        args_dict = json.loads(data)
        return args_dict['args'], args_dict['kwargs']

    @staticmethod
    def serialize_workflow_result(result):
        """ Serialize the given workflow *result*. """
        return json.dumps(result)


class BaseProxy(object):
    @staticmethod
    def serialize_input(*args, **kwargs):
        """ Serialize the given activity *args* and *kwargs*. """
        return json.dumps({'args': args, 'kwargs': kwargs})

    @staticmethod
    def deserialize_result(result):
        """ Deserialize the given *result*. """
        return json.loads(result)

    def _queue(self, workflow_execution, input):
        raise NotImplemented()

    def __call__(self, wf_exec, *args, **kwargs):
        wf_exec.next_call()

        err = _args_error(args, kwargs)
        if err is not None:
            try:
                err.result()
            except TaskError as e:
                wf_exec.fail('Proxy called with error result: %s' % e.message)
                return Placeholder()

        if _any_placeholders(args, kwargs):
            return Placeholder()

        if wf_exec.current_call_running():
            return Placeholder()

        if wf_exec.current_call_timedout():
            if wf_exec.should_retry():
                input = self.serialize_input(*args, **kwargs)
                self._queue(wf_exec, input)
                return Placeholder()
            else:
                if wf_exec.error_handling():
                    return Timeout()
                wf_exec.fail('An activity timed out.')
                return Placeholder()

        error_message = wf_exec.current_call_error()
        if error_message is not None:
            if wf_exec.error_handling():
                return Error(error_message)
            wf_exec.fail('Error in activity: %s' % error_message)
            return Placeholder()

        result = wf_exec.current_call_result()
        if result is not None:
            return Result(self.deserialize_result(result))

        self._queue(wf_exec, self.serialize_input(*args, **kwargs))
        return Placeholder()


class ActivityProxy(BaseProxy):
    """ The object that represents an activity from the workflows point of
    view.

    Defines an activity with the given *name*, *version* and configuration
    options. The configuration options set here have a higher priority than
    the ones set when registering an activity.
    As far as the workflow that instantiates objects of this type is concerned,
    it has no relevance where these activities are processed, or in what
    manner.

    """
    def __init__(self, name, version,
                 heartbeat=None, schedule_to_close=None,
                 schedule_to_start=None, start_to_close=None,
                 task_list=None, retry=3):
        self.name = name
        self.version = version
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.start_to_close = start_to_close
        self.task_list = task_list
        self.retry = retry

    def _queue(self, workflow_execution, input):
        return workflow_execution.queue_activity(
            name=self.name,
            version=self.version,
            input=input,
            heartbeat=self.heartbeat,
            schedule_to_close=self.schedule_to_close,
            schedule_to_start=self.schedule_to_start,
            start_to_close=self.start_to_close,
            task_list=self.task_list,
            retry=self.retry
        )


class WorkflowProxy(BaseProxy):
    def __init__(self, name, version,
                 task_start_to_close=None, execution_start_to_close=None,
                 task_list=None, retry=3):
        self.name = name
        self.version = version
        self.task_start_to_close = task_start_to_close
        self.execution_start_to_close = execution_start_to_close
        self.task_list = task_list
        self.retry = retry

    def _queue(self, workflow_execution, input):
        return workflow_execution.queue_childworkflow(
            name=self.name,
            version=self.version,
            input=input,
            task_start_to_close=self.task_start_to_close,
            execution_start_to_close=self.execution_start_to_close,
            task_list=self.task_list,
            retry=self.retry
        )


_OBase = namedtuple(
    typename='_Options',
    field_names=[
        'heartbeat',
        'schedule_to_close',
        'schedule_to_start',
        'start_to_close',
        'task_start_to_close',
        'execution_start_to_close',
        'task_list',
        'retry'
    ]
)


class _Options(_OBase):
    def update_with(self, other):
        t_pairs = zip(other, self)
        updated_fields = [x if x is not None else y for x, y in t_pairs]
        return _Options(*updated_fields)


class _SyncNeeded(Exception):
    """Stops the workflow execution when an activity result is unavailable."""


class TaskError(RuntimeError):
    """Raised if manual handling is ON if there is a problem in a task."""


class TaskTimedout(TaskError):
    """Raised if manual handling is ON on task timeout."""


def _any_placeholders(args, kwargs):
    a = list(args) + list(kwargs.items())
    return any(isinstance(r, Placeholder) for r in a)


def _args_error(args, kwargs):
    a = list(args) + list(kwargs.items())
    errs = list(filter(lambda x: isinstance(x, Error), a))
    if errs:
        return errs[0]
