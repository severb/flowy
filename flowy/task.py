import json
from functools import partial

from flowy import posint_or_none, str_or_none


class SuspendTask(Exception):
    """ Raised to suspend the task run.

    This happens when a worklfow needs to wait for an activity or in case of an
    async activity.
    """


class TaskError(Exception):
    """ Raised from an activity or subworkflow task if error handling is
    enabled and the task fails.
    """


class TaskTimedout(TaskError):
    """ Raised from an activity or subworkflow task if any of its timeout
    timers were exceeded.
    """


class Task(object):
    def __init__(self, input, runtime):
        self._input = str(input)
        self._runtime = runtime

    def __call__(self):
        try:
            args, kwargs = self._deserialize_arguments()
            result = self.run(*args, **kwargs)
        except SuspendTask:
            self._runtime.suspend()
        except Exception as e:
            self._runtime.fail(str(e))
        else:
            self._runtime.complete(self._serialize_result(result))

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def _serialize_result(self, result):
        return json.dumps(result)

    def _deserialize_arguments(self):
        return json.loads(self._input)


class Activity(Task):
    def heartbeat(self):
        return self._runtime.heartbeat()


class Workflow(Task):
    def options(self, **kwargs):
        self._runtime.options(**kwargs)

    def restart(self, *args, **kwargs):
        arguments = self.serialize_restart_arguments(*args, **kwargs)
        return self._runtime.restart(arguments)

    def _serialize_restart_arguments(self, *args, **kwargs):
        return json.dumps([args, kwargs])


class TaskProxy(object):
    def __get__(self, obj, objtype):
        if obj is None:
            return self
        if not hasattr(obj, '_runtime'):
            raise AttributeError('no runtime bound to the task')
        return partial(self, obj._runtime)

    def _serialize_arguments(self, *args, **kwargs):
        return json.dumps([args, kwargs])

    def _deserialize_result(self, result):
        return json.loads(result)


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
            stat_to_close=posint_or_none(start_to_close),
            task_list=str_or_none(task_list),
            retry=max(int(retry), 0),
            delay=max(int(delay), 0),
            error_handling=bool(error_handling)
        )

    def __call__(self, runtime, *args, **kwargs):
        return runtime.remote_activity(
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

    def __call__(self, runtime, *args, **kwargs):
        return runtime.remote_subworkflow(
            args=args, kwargs=kwargs,
            args_serializer=self._serialize_arguments,
            result_deserializer=self._deserialize_result,
            **self._kwargs
        )
