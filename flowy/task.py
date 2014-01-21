import json

from flowy import int_or_none, str_or_none


class SuspendTask(Exception):
    """ Raised to suspend the task run.

    This happens when a worklfow needs to wait for an activity or in case of an
    async activity.
    """


class Task(object):
    def __init__(self, input, result, task_runtime=None):
        self._input = str(input)
        self._result = result
        self._task_runtime = task_runtime

    def bind_task_runtime(self, task_runtime):
        self._task_runtime = task_runtime

    def __call__(self):
        try:
            args, kwargs = self.deserialize_arguments()
            result = self.run(self._task_runtime, *args, **kwargs)
        except SuspendTask:
            self._result.suspend()
        except Exception as e:
            self._result.fail(str(e))
        else:
            self._result.complete(self.serialize_result(result))

    def run(self, runtime, *args, **kwargs):
        raise NotImplementedError

    def serialize_result(self, result):
        return json.dumps(result)

    def deserialize_arguments(self):
        return json.loads(self._input)


class TaskProxy(object):
    def serialize_arguments(self, *args, **kwargs):
        return json.dumps([args, kwargs])

    def deserialize_result(self, result):
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
            heartbeat=int_or_none(heartbeat),
            schedule_to_close=int_or_none(schedule_to_close),
            schedule_to_start=int_or_none(schedule_to_start),
            stat_to_close=int_or_none(start_to_close),
            task_list=str_or_none(task_list),
            retry=int(retry),
            delay=int(delay),
            error_handling=bool(error_handling)
        )

    def __call__(self, runtime, *args, **kwargs):
        arguments = self.serialize_arguments(*args, **kwargs)
        return runtime.remote_activity(input=arguments, **self._kwargs)


class SubworkflowProxy(TaskProxy):
    def __init__(self, task_id,
                 decision_duration=None,
                 workflow_duration=None,
                 task_list=None,
                 retry=3,
                 delay=0,
                 error_handling=False):
        self._kwargs = dict(
            task_id=task_id,
            decision_duration=int_or_none(decision_duration),
            workflow_duration=int_or_none(workflow_duration),
            task_list=str_or_none(task_list),
            retry=int(retry),
            delay=int(delay),
            error_handling=bool(error_handling)
        )

    def __call__(self, runtime, *args, **kwargs):
        arguments = self.serialize_arguments(*args, **kwargs)
        return runtime.remote_subworkflow(input=arguments, **self._kwargs)
