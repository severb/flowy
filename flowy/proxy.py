import json
from contextlib import contextmanager

from flowy.exception import TaskError
from flowy.result import Error
from flowy.result import Placeholder
from flowy.result import Result
from flowy.result import Timeout
from flowy.spec import _sentinel
from flowy.spec import serialize_arguments
from flowy.spec import SWFActivitySpec
from flowy.spec import SWFWorkflowSpec
from flowy.util import MagicBind


deserialize_result = json.loads


class TaskProxy(object):

    def __init__(self, retry=[0, 0, 0], error_handling=False,
                 deserialize_result=deserialize_result):
        self._retry = retry
        self._error_handling = error_handling
        self._deserialize_result = deserialize_result

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return MagicBind(self, workflow=obj)

    @contextmanager
    def options(self, retry=_sentinel, error_handling=_sentinel):
        old_retry = self._retry
        old_error_handling = self._error_handling
        if retry is not _sentinel:
            self._retry = retry
        if error_handling is not _sentinel:
            self._error_handling = error_handling
        yield
        self._retry = old_retry
        self._error_handling = old_error_handling

    def __call__(self, workflow, *args, **kwargs):
        raise NotImplementedError


class ActivityProxy(TaskProxy):
    def __call__(self, workflow, *args, **kwargs):
        sched = workflow._schedule_activity
        if self._error_handling:
            sched = workflow._schedule_activity_with_err
        return sched(self, args, kwargs, self._retry, self._deserialize_result)


class WorkflowProxy(TaskProxy):
    def __call__(self, workflow, *args, **kwargs):
        sched = workflow._schedule_workflow
        if self._error_handling:
            sched = workflow._schedule_workflow_with_err
        return sched(self, args, kwargs, self._retry, self._deserialize_result)


class SWFActivityProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, retry=[0, 0, 0], error_handling=False,
                 serialize_arguments=serialize_arguments,
                 deserialize_result=deserialize_result):
        self._spec = SWFActivitySpec(name, version, task_list, heartbeat,
                                     schedule_to_close, schedule_to_start,
                                     start_to_close, serialize_arguments)
        super(SWFActivityProxy, self).__init__(retry, error_handling,
                                               deserialize_result)

    @contextmanager
    def options(self, task_list=_sentinel, heartbeat=_sentinel,
                schedule_to_close=_sentinel, schedule_to_start=_sentinel,
                start_to_close=_sentinel, retry=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, heartbeat, schedule_to_close,
                                schedule_to_start, start_to_close):
            with super(SWFActivityProxy, self).options(retry, error_handling):
                yield

    def schedule(self, scheduler, *args, **kwargs):
        return self._spec.schedule(*args, **kwargs)


class SWFWorkflowProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None, retry=[0, 0, 0], error_handling=False,
                 serialize_arguments=serialize_arguments,
                 deserialize_result=deserialize_result):
        self._spec = SWFWorkflowSpec(name, version, task_list,
                                     decision_duration, workflow_duration,
                                     serialize_arguments)
        super(SWFWorkflowProxy, self).__init__(retry, error_handling,
                                               deserialize_result)

    def schedule(self, *args, **kwargs):
        return self._spec.schedule(*args, **kwargs)

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, retry=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            with super(SWFWorkflowProxy, self).options(retry, error_handling):
                yield

    def schedule(self, swf_decisions, call_key, a, kw):
        return self._spec.schedule(swf_decisions, call_key, a, kw)
