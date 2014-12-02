import json
from contextlib import contextmanager

from flowy.util import MagicBind
from flowy.task import extract_results


_sentinel = object()


class TaskProxy(object):

    from flowy.result import Placeholder
    from flowy.result import Result
    from flowy.result import Error
    from flowy.result import LinkedError
    from flowy.result import Timeout

    def __init__(self, name, retry=[0, 0, 0]):
        self._name = name
        self._retry = retry

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return MagicBind(self, workflow=obj)

    @contextmanager
    def options(self, retry=_sentinel):
        new_retry = self._retry
        if retry is not _sentinel:
            new_retry = retry
        yield TaskProxy(self._name, new_retry)

    def __call__(self, workflow, *args, **kwargs):
        return wolkflow._lookup(self, args, kwargs)

    def __iter__(self):
        return iter(self._retry)


class ActivityProxy(TaskProxy):
    def __init__(self, name, retry=[0, 0, 0], heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None):
        self._heartbeat = heartbeat
        self._schedule_to_close = schedule_to_close
        self._schedule_to_start = schedule_to_start
        self._start_to_close = start_to_close
        super(ActivityPorxy, self).__init__(name, retry)

    @contextmanager
    def options(self, heartbeat=_sentinel, schedule_to_close=_sentinel,
                schedule_to_start=_sentinel, start_to_close=_sentinel,
                retry=_sentinel):
        new_heartbeat = self._heartbeat
        new_schedule_to_close = self._schedule_to_close
        new_schedule_to_start = self._schedule_to_start
        new_start_to_close = self._start_to_close
        new_retry = retry
        if heartbeat is not _sentinel:
            new_heartbeat = heartbeat
        if schedule_to_close is not _sentinel:
            new_schedule_to_close = schedule_to_close
        if schedule_to_start is not _sentinel:
            new_schedule_to_start = schedule_to_start
        if start_to_close is not _sentinel:
            new_start_to_close = start_to_close
        if retry is not _sentinel:
            new_retry = _retry
        klass = self.__class__
        yield klass(self._name, new_retry, new_heartbeat,
                    new_schedule_to_close, new_schedule_to_start,
                    new_start_to_close)


class WorkflowProxy(TaskProxy):
    def __init__(self, name, retry=[0, 0, 0], decision_duration=None,
                 workflow_duration=None):
        self._decision_duration = decision_duration
        self._workflow_duration = workflow_duration
        super(ActivityPorxy, self).__init__(name, retry)

    @contextmanager
    def options(self, decision_duration=_sentinel,
                workflow_duration=_sentinel, retry=_sentinel):
        new_decision_duration = self._decision_duration
        new_workflow_duration = self._workflow_duration
        new_retry = retry
        if decision_duration is not _sentinel:
            new_decision_duration = decision_duration
        if workflow_duration is not _sentinel:
            new_workflow_duration = workflow_duration
        if retry is not _sentinel:
            new_retry = _retry
        klass = self.__class__
        yield klass(self._name, new_retry, new_decision_duration,
                    new_workflow_duration)
