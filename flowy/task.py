import json
from functools import partial

from boto.swf.exceptions import SWFResponseError
from boto.swf.layer1_decisions import Layer1Decisions
from flowy import logger, posint_or_none, str_or_none
from flowy.exception import SuspendTask, TaskError
from flowy.result import Error, Placeholder, Result, Timeout


serialize_result = staticmethod(json.dumps)
deserialize_result = staticmethod(json.loads)
deserialize_args = staticmethod(json.loads)


@staticmethod
def serialize_args(*args, **kwargs):
    return json.dumps([args, kwargs])


class Task(object):
    def __init__(self, input, token):
        self._input = input
        self._token = token

    @property
    def token(self):
        return str(self._token)

    def __call__(self):
        try:
            args, kwargs = self._deserialize_arguments(self._input)
        except ValueError:
            logger.exception("Error while deserializing the arguments:")
            return False
        try:
            result = self.run(*args, **kwargs)
        except SuspendTask:
            return self._suspend()
        except Exception as e:
            logger.exception("Error while running the task:")
            return self._fail(e)
        else:
            return self._finish(result)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def _suspend(self):
        raise NotImplementedError

    def _fail(self, reason):
        raise NotImplementedError

    def _finish(self, result):
        raise NotImplementedError

    _serialize_result = serialize_result
    _deserialize_arguments = deserialize_args


class SWFActivity(Task):
    def __init__(self, swf_client, input, token):
        self._swf_client = swf_client
        super(SWFActivity, self).__init__(input, token)

    def _suspend(self):
        return True

    def _fail(self, reason):
        return _activity_fail(self._swf_client, self.token, reason)

    def _finish(self, result):
        try:
            result = self._serialize_result(result)
        except TypeError:
            logger.exception('Could not serialize result:')
            return False
        return _activity_finish(self._swf_client, self.token, result)

    def heartbeat(self):
        return _activity_heartbeat(self._swf_client, self.token)


class AsyncSWFActivity(object):
    def __init__(self, swf_client, token):
        self._swf_client = swf_client
        self._token = token

    def heartbeat(self):
        return _activity_heartbeat(self._swf_client, self._token)

    def fail(self, reason):
        return _activity_fail(self._swf_client, self._token)

    def finish(self, result):
        try:
            result = self._serialize_result(result)
        except TypeError:
            logger.exception('Could not serialize result:')
            return False
        return _activity_finish(self._swf_client, self._token, result)

    _serialize_result = serialize_result


def _activity_heartbeat(swf_client, token):
    try:
        swf_client.record_activity_task_heartbeat(task_token=str(token))
    except SWFResponseError:
        logger.exception('Error while sending the heartbeat:')
        return False
    return True


def _activity_fail(swf_client, token, reason):
    try:
        swf_client.respond_activity_task_failed(
            reason=str(reason)[:256], task_token=str(token))
    except SWFResponseError:
        logger.exception('Error while failing the activity:')
        return False
    return True


def _activity_finish(swf_client, token, result):
    try:
        swf_client.respond_activity_task_completed(
            result=str(result), task_token=str(token))
    except SWFResponseError:
        logger.exception('Error while completing the activity:')
        return False
    return True


class Workflow(Task):
    def __init__(self, swf_client, input, token, spec, tags=None):
        self._swf_client = swf_client
        self._tags = tags
        self._spec = spec
        self._decisions = Layer1Decisions()
        self._closed = False
        super(Workflow, self).__init__(input, token)

    def options(self):  # change restart options, including tags
        pass

    def restart(self, *args, **kwargs):
        try:
            input = self._serialize_restart_arguments(*args, **kwargs)
        except TypeError:
            logger.exception('Error while serializing restart arguments:')
            return False
        decisions = self._decisions = Layer1Decisions()
        self._spec.restart(decisions, input, self._tags)
        return self._suspend()

    def _suspend(self):
        if self._closed:
            return False
        self._closed = True
        try:
            self._swf_client.respond_decision_task_completed(
                task_token=self.token, decisions=self._decisions._data
            )
            return True
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')
            return False

    def _fail(self, reason):
        decisions = self._decisions = Layer1Decisions()
        decisions.fail_workflow_execution(reason=str(reason)[:256])
        return self._suspend()

    def _finish(self, result):
        r = result
        if isinstance(result, Result):
            r = result.result()
        elif isinstance(result, (Error, Timeout)):
            try:
                result.result()
            except TaskError as e:
                return self._fail(e)
        # No need to cover this case - if we get a placeholder it must be
        # because something is running or is scheduled and the next condition
        # won't pass anyway
        # elif isinstance(result, Placeholder):
        #     return self._suspend()
        if self._nothing_scheduled() and self._nothing_running():
            try:
                r = self._serialize_result(r)
            except TypeError:
                logger.exception("Error while serializing the result:")
                return False
            decisions = self._decisions = Layer1Decisions()
            decisions.complete_workflow_execution(result=r)
        return self._suspend()

    def _nothing_scheduled(self):
        return not self._decisions._data

    def _nothing_running(self):
        return not True

    def schedule_activity(self, *args, **kwargs):
        pass

    def schedule_workflow(self, *args, **kwargs):
        pass

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
