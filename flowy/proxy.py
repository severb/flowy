import json
from contextlib import contextmanager

from flowy.exception import TaskError
from flowy.result import Error, Placeholder, Result, Timeout
from flowy.spec import _sentinel, SWFActivitySpec, SWFWorkflowSpec
from flowy.task import serialize_args
from flowy.util import MagicBind


deserialize_result = staticmethod(json.loads)


class TaskProxy(object):

    timeout_message = "A task has timed-out"

    def __init__(self, retry=3, delay=0, error_handling=False):
        self._retry = retry
        self._delay = delay
        self._error_handling = error_handling

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return MagicBind(self, task=obj)

    @contextmanager
    def options(self, retry=_sentinel, delay=_sentinel,
                error_handling=_sentinel):
        old_retry = self._retry
        old_delay = self._delay
        old_error_handling = self._error_handling
        if retry is not _sentinel:
            self._retry = retry
        if delay is not _sentinel:
            self._delay = delay
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
        state, value = self._schedule(task, input)
        if state == task._FOUND:
            return Result(self._deserialize_result(value))
        elif state == task._RUNNING:
            return Placeholder()
        elif state == task._ERROR:
            if self._error_handling:
                return Error(value)
            task.fail(value)
            return Placeholder()
        elif state == task._TIMEDOUT:
            if self._error_handling:
                return Timeout()
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
        self.timeout_message = "Activity %s has timed-out" % self._spec
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

    def _schedule(self, task, input):
        return task.schedule_activity(self._spec, input, self._retry,
                                      self._delay)


class SWFWorkflowProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None, retry=3, delay=0,
                 error_handling=False):
        self._spec = SWFWorkflowSpec(name, version, task_list,
                                     decision_duration, workflow_duration)
        self.timeout_message = "Workflow %s has timed-out" % self._spec
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

    def _schedule(self, task, input):
        return task.schedule_workflow(self._spec, input, self._retry,
                                      self._delay)
