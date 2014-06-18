import json
from contextlib import contextmanager
from functools import partial

from flowy.exception import TaskError, TaskTimedout
from flowy.results import Error, Placeholder, Result, Timeout
from flowy.spec import _sentinel, SWFActivitySpec, SWFWorkflowSpec
from flowy.task import serialize_args


deserialize_result = staticmethod(json.loads)


class TaskProxy(object):

    timeout_message = "A task has timed-out"
    error_message = "Error in task: %s"

    def __init__(self, retry=3, delay=0, error_handling=False):
        self._retry = retry
        self._delay = delay
        self._error_handling = error_handling

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return partial(self, obj)

    @contextmanager
    def options(self, retry=_sentinel, delay=_sentinel,
                error_handling=_sentinel):
        old_retry = self._retry
        old_delay = self._delay
        old_error_handling = self._error_handling
        if retry is not _sentinel:
            self._retry = retry
        if delay is not _sentinel:
            self._delay = self._delay
        if error_handling is not _sentinel:
            self._error_handling = error_handling
        yield
        self._retry = old_retry
        self._delay = old_delay
        self._error_handling = old_error_handling

    def __call__(self, task, *args, **kwargs):
        result = self._args_based_result(task, args, kwargs)
        if result is not None:
            return result
        args, kwargs = self._extract_results(args, kwargs)
        # there is no error handling for argument/result transport
        # we want those to bubble up in the workflow and stop it
        input = self._serialize_arguments(*args, **kwargs)
        try:
            placeholder = Placeholder()
            result = self._schedule(task, input, default=placeholder)
            if result is placeholder:
                return placeholder
            return Result(self._deserialize_result(result))
        except TaskError as e:
            if self._error_handling:
                return Error(str(e))
            else:
                task.fail(self.error_message % e)
                return Placeholder()
        except TaskTimedout:
            if self._error_handling:
                return Timeout()
            else:
                task.fail(self.timeout_message)
                return Placeholder()

    def _args_based_result(self, task, args, kwargs):
        args = tuple(args) + tuple(kwargs.values())
        error_message = self._errs_in_args(args)
        if error_message:
            if self._error_handling:
                return Error(error_message)
            else:
                task.fail(error_message)
                return Placeholder()
        if self._deps_in_args(args):
            return Placeholder()

    def _deps_in_args(self, args):
        return any(isinstance(r, Placeholder) for r in args)

    def _errs_in_args(self, args):
        errors = []
        for arg in args:
            if isinstance(arg, Error):
                try:
                    arg.result()
                except TaskError as e:
                    errors.append(str(e))
        return '\n'.join(errors)

    def _extract_results(self, args, kwargs):
        a = [arg.result() if isinstance(arg, Result)
             else arg for arg in args]
        k = dict((k, v.result() if isinstance(v, Result) else v)
                 for k, v in kwargs.items())
        return a, k

    _serialize_arguments = serialize_args
    _deserialize_result = deserialize_result


class SWFActivityProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, retry=3, delay=0, error_handling=False):
        self._spec = SWFActivitySpec(name, version, task_list, heartbeat,
                                     schedule_to_close, schedule_to_start,
                                     start_to_close)
        self.timeout_message = "Activity %r has timed-out" % self._spec
        self.error_message = "Error in activity %r: %%s" % self._spec
        super(SWFActivityProxy, self).__init__(retry, delay, error_handling)

    @contextmanager
    def options(self, task_list=_sentinel, heartbeat=_sentinel,
                schedule_to_close=_sentinel, schedule_to_start=_sentinel,
                start_to_close=_sentinel, retry=_sentinel, delay=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, heartbeat, schedule_to_close,
                                schedule_to_start, start_to_close):
            with super(SWFActivityProxy, self).options(retry, delay,
                                                       error_handling):
                yield

    def schedule(self, task, input, default=None):
        return task.schedule_activity(self._spec, input, self._retry,
                                      self._delay, default=default)


class SWFWorkflowProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None, retry=3, delay=0,
                 error_handling=False):
        self._spec = SWFWorkflowSpec(name, version, task_list,
                                     decision_duration, workflow_duration)
        self.timeout_message = "Workflow %r has timed-out" % self._spec
        self.error_message = "Error in workflow %r: %%s" % self._spec
        super(SWFWorkflowProxy, self).__init__(retry, delay, error_handling)

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, retry=_sentinel, delay=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            with super(SWFWorkflowProxy, self).options(retry, delay,
                                                       error_handling):
                yield

    def schedule(self, task, input, default=None):
        return task.schedule_workflow(self._spec, input, self._retry,
                                      self._delay, default=default)
