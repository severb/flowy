import json
from functools import partial

from flowy import posint_or_none, str_or_none


deserialize_result = staticmethod(json.loads)


@staticmethod
def serialize_args(*args, **kwargs):
    return json.dumps([args, kwargs])


class TaskProxy(object):
    def __get__(self, obj, objtype):
        if obj is None:
            return self
        if not hasattr(obj, '_scheduler'):
            raise AttributeError('no scheduler bound to the task')
        return partial(self, obj._scheduler)

    _serialize_arguments = serialize_args
    _deserialize_result = deserialize_result


class ActivityProxy(TaskProxy):
    def __init__(self, task_id,
                 heartbeat=None,
                 schedule_to_close=None,
                 schedule_to_start=None,
                 start_to_close=None,
                 task_list=None,
                 retry=3,
                 delay=0,
                 error_handling=False):
        self._kwargs = dict(
            task_id=task_id,
            heartbeat=posint_or_none(heartbeat),
            schedule_to_close=posint_or_none(schedule_to_close),
            schedule_to_start=posint_or_none(schedule_to_start),
            start_to_close=posint_or_none(start_to_close),
            task_list=str_or_none(task_list),
            retry=max(int(retry), 0),
            delay=max(int(delay), 0),
            error_handling=bool(error_handling)
        )

    def __call__(self, scheduler, *args, **kwargs):
        return scheduler.remote_activity(
            args=args, kwargs=kwargs,
            args_serializer=self._serialize_arguments,
            result_deserializer=self._deserialize_result,
            **self._kwargs
        )


class WorkflowProxy(TaskProxy):
    def __init__(self, task_id,
                 decision_duration=None,
                 workflow_duration=None,
                 task_list=None,
                 retry=3,
                 delay=0,
                 error_handling=False):
        self._kwargs = dict(
            task_id=task_id,
            decision_duration=posint_or_none(decision_duration),
            workflow_duration=posint_or_none(workflow_duration),
            task_list=str_or_none(task_list),
            retry=max(int(retry), 0),
            delay=max(int(delay), 0),
            error_handling=bool(error_handling)
        )

    def __call__(self, scheduler, *args, **kwargs):
        return scheduler.remote_subworkflow(
            args=args, kwargs=kwargs,
            args_serializer=self._serialize_arguments,
            result_deserializer=self._deserialize_result,
            **self._kwargs
        )
