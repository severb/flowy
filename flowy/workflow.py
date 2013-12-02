import json
from collections import namedtuple
from contextlib import contextmanager
from functools import partial

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


class WorkflowExecution(object):
    def __init__(self, decision, execution_history):

        self._decision = decision

        default = _Options(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            task_start_to_close=None,
            execution_start_to_close=None,
            task_list=None,
            retry=3,
            delay=0,
        )
        self._options_stack = [default]
        self._error_handling_stack = [False]
        self._call_id = 0

    @contextmanager
    def options(self, heartbeat=None, schedule_to_close=None,
                schedule_to_start=None, start_to_close=None,
                error_handling=None, task_start_to_close=None,
                execution_start_to_close=None, task_list=None,
                retry=None, delay=None):
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
            retry=retry,
            delay=delay,
        )
        new_options = self._current_options.update_with(options)
        self._options_stack.append(new_options)
        yield
        self._options_stack.pop()
        if error_handling is not None:
            self._error_handling_stack.pop()

    def queue_childworkflow(self, name, version, input,
                            task_start_to_close=None,
                            execution_start_to_close=None,
                            task_list=None, retry=None, delay=None):
        workflow_options = _Options(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            task_start_to_close=task_start_to_close,
            execution_start_to_close=execution_start_to_close,
            task_list=task_list,
            retry=None,
            delay=None,
        )

        options = workflow_options.update_with(self._current_options)
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

    def activity_call(self, name, version, args, kwargs, transport,
                      heartbeat=None, schedule_to_close=None,
                      schedule_to_start=None, start_to_close=None,
                      task_list=None, retry=None, delay=None):
        activity_options = _Options(
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_start_to_close=None,
            execution_start_to_close=None,
            task_list=task_list,
            retry=retry,
            delay=delay,
        )

        # Context settings have the highest priority,
        # even higher than the ones sent as arguments in this method!
        options = activity_options.update_with(self._current_options)

        call_id = self._call_id
        delay = int(options.delay)
        retry = max(int(options.retry), 0)

        # Reserve the call_ids need by this call
        self._call_id = (
            1 + call_id  # one for the first call
            + int(options.delay > 0)  # one for the timer if needed
            + retry  # one for each possible retry
        )

        err = _args_error(tuple(args) + tuple(kwargs.items()))
        if err is not None:
            try:
                err.result()
            except TaskError as e:
                self.decision.fail(
                    'Proxy called with error result: %s' % e.message
                )
                return Placeholder()

        if _any_placeholders(tuple(args) + tuple(kwargs.items())):
            return Placeholder()

        if delay > 0:
            if self._is_running(str(call_id)):
                return Placeholder()
            if not self._decision.is_fired(str(call_id)):
                # if not running and not fired it must be queued
                self._decision.queue_timer(options.delay)
                return Placeholder()
            call_id += 1

        for call_id in range(call_id, retry + 1):
            if self.decision.is_timeout(str(call_id)):
                continue
            if self.decision.is_running(str(call_id)):
                return Placeholder()
            error_message = self.decision.get_error(call_id)
            if error_message is not None:
                if self._error_handling:
                    return Error(error_message)
                self.decision.fail('Error in activity: %s' % error_message)
                return Placeholder()
            result = self.decision.get_result(call_id)
            if result is not None:
                return Result(transport.deserialize_result(result))
            self._decision.queue_activity(
                call_id=str(call_id),
                name=name,
                version=version,
                input=transport.serialize_input(*args, **kwargs),
                heartbeat=options.heartbeat,
                schedule_to_close=options.schedule_to_close,
                schedule_to_start=options.schedule_to_start,
                start_to_close=options.start_to_close,
                task_list=options.task_list,
            )

        # Well we reached the max retrying
        if self._error_handling():
            return Timeout()
        self.decision.fail('An activity timed out.')
        return Placeholder()

    @property
    def _error_handling(self):
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
        if not callable(task_proxy):
            raise AttributeError('%r is not a be callable' % task_name)
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

        we = WorkflowExecution(decision)
        remote = BoundInstance(self, we)

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

        if we.is_completed():
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


class JSONTransportProxy(object):
    @staticmethod
    def serialize_input(*args, **kwargs):
        """ Serialize the given activity *args* and *kwargs*. """
        return json.dumps({'args': args, 'kwargs': kwargs})

    @staticmethod
    def deserialize_result(result):
        """ Deserialize the given *result*. """
        return json.loads(result)


class ActivityProxy(JSONTransportProxy):
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
                 task_list=None, retry=None, delay=None):
        self.name = name
        self.version = version
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.start_to_close = start_to_close
        self.task_list = task_list
        self.retry = retry
        self.delay = delay

    def __call__(self, workflow_execution, *args, **kwargs):
        return workflow_execution.activity_call(
            name=self.name,
            version=self.version,
            heartbeat=self.heartbeat,
            schedule_to_close=self.schedule_to_close,
            schedule_to_start=self.schedule_to_start,
            task_list=self.task_list,
            retry=self.retry,
            delay=self.delay,
            args=args,
            kwargs=kwargs,
            transport=self,
        )


class WorkflowProxy(JSONTransportProxy):
    def __init__(self, name, version,
                 task_start_to_close=None, execution_start_to_close=None,
                 task_list=None, retry=None, delay=None):
        self.name = name
        self.version = version
        self.task_start_to_close = task_start_to_close
        self.execution_start_to_close = execution_start_to_close
        self.task_list = task_list
        self.retry = retry
        self.delay = delay

    def __call__(self, workflow_execution, *args, **kwargs):
        return workflow_execution.subworkflow_call(
            name=self.name,
            version=self.version,
            task_start_to_close=self.task_start_to_close,
            execution_start_to_close=self.execution_start_to_close,
            task_list=self.task_list,
            retry=self.retry,
            delay=self.delay,
            args=args,
            kwargs=kwargs,
            transport=self,
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
        'retry',
        'delay',
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


def _any_placeholders(a):
    return any(isinstance(r, Placeholder) for r in a)


def _args_error(a):
    errs = list(filter(lambda x: isinstance(x, Error), a))
    if errs:
        return errs[0]
