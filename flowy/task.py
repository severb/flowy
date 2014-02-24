import json
from functools import partial

from flowy import logger, posint_or_none, str_or_none
from flowy.result import Result, Error, Timeout, Placeholder
from flowy.exception import SuspendTask, TaskError


serialize_result = staticmethod(json.dumps)
deserialize_result = staticmethod(json.loads)
deserialize_args = staticmethod(json.loads)


@staticmethod
def serialize_args(*args, **kwargs):
    return json.dumps([args, kwargs])


class Task(object):
    def __init__(self, input, scheduler):
        self._args, self._kwargs = self._deserialize_arguments(str(input))
        self._scheduler = scheduler

    def __call__(self):
        try:
            result = self.run(*self._args, **self._kwargs)
        except SuspendTask:
            self._scheduler.suspend()
        except Exception as e:
            self._scheduler.fail(str(e))
            logger.exception("Error while running the task:")
        else:
            return self._finish(result)

    def _finish(self, result):
        self._scheduler.complete(self._serialize_result(result))

    def run(self, *args, **kwargs):
        raise NotImplementedError

    _serialize_result = serialize_result
    _deserialize_arguments = deserialize_args


class Activity(Task):
    def heartbeat(self):
        return self._scheduler.heartbeat()


class Workflow(Task):
    def options(self, **kwargs):
        return self._scheduler.options(**kwargs)

    def restart(self, *args, **kwargs):
        arguments = self._serialize_restart_arguments(*args, **kwargs)
        return self._scheduler.restart(arguments)

    def _finish(self, result):
        r = result
        if isinstance(result, Result):
            r = result.result()
        elif isinstance(result, (Error, Timeout)):
            try:
                result.result()
            except TaskError as e:
                return self._scheduler.fail(str(e))
        elif isinstance(result, Placeholder):
            return self._scheduler.suspend()
        return super(Workflow, self)._finish(r)

    _serialize_restart_arguments = serialize_args


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
