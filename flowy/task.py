# import json


class SuspendTask(Exception):
    """ Raised to suspend the task run.

    This happens when a worklfow needs to wait for an activity or in case of an
    async activity.
    """


class Task(object):
    def __init__(self, input, result, task_runtime=None):
        self._input = input
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
    def __init__(self, name, version,
                 heartbeat=None,
                 schedule_to_close=None,
                 schedule_to_start=None,
                 start_to_close=None,
                 task_list=None,
                 retry=3,
                 delay=0):
        self._name = name
        self._version = version
        self._heartbeat = heartbeat
        self._schedule_to_close = schedule_to_close
        self._schedule_to_start = schedule_to_start
        self._stat_to_close = start_to_close
        self._task_list = task_list
        self._retry = retry
        self._delay = delay

    def __call__(self, runtime, *args, **kwargs):
        arguments = self.serialize_arguments(*args, **kwargs)
        return runtime.remote_activity(
            name=self._name,
            version=self._version,
            task_list=self._task_list,
            input=arguments,
            heartbeat=self._heartbeat,
            schedule_to_close=self._schedule_to_close,
            schedule_to_start=self._schedule_to_start,
            start_to_close=self._stat_to_close,
            retry=self._retry,
            delay=self._delay,
            result_deserializer=self.deserialize_result
        )


class SubworkflowProxy(TaskProxy):
    def __init__(self, name, version,
                 decision_duration=None,
                 workflow_duration=None,
                 task_list=None,
                 retry=3,
                 delay=0):
        self._name = name
        self._version = version
        self._decision_duration = decision_duration
        self._workflow_duration = workflow_duration
        self._task_list = task_list
        self._retry = retry
        self._delay = delay

    def __call__(self, runtime, *args, **kwargs):
        arguments = self.serialize_arguments(*args, **kwargs)
        return runtime.remote_subworkflow(
            name=self._name,
            version=self._version,
            task_list=self._task_list,
            input=arguments,
            decision_duration=self._decision_duration,
            workflow_duration=self._workflow_duration,
            retry=self._retry,
            delay=self._delay,
            result_deserializer=self.deserialize_result
        )
