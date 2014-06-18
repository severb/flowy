import json
import uuid
from contextlib import contextmanager

from boto.swf.exceptions import SWFResponseError
from boto.swf.layer1_decisions import Layer1Decisions
from flowy import logger
from flowy.exception import SuspendTask, TaskError, TaskTimedout
from flowy.result import Error, Result, Timeout
from flowy.spec import _sentinel


serialize_result = staticmethod(json.dumps)
deserialize_args = staticmethod(json.loads)

_TIMEDOUT, _RUNNING, _ERROR, _FOUND, _NOTFOUND = range(5)


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
            return self.fail(e)
        else:
            return self._finish(result)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def _suspend(self):
        raise NotImplementedError

    def fail(self, reason):
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

    def fail(self, reason):
        return _activity_fail(self._swf_client, self.token, reason)

    def _finish(self, result):
        try:
            result = self._serialize_result(result)
        except TypeError:
            logger.exception('Error while serializing the result:')
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
        return _activity_fail(self._swf_client, self._token, reason)

    def finish(self, result):
        try:
            result = self._serialize_result(result)
        except TypeError:
            logger.exception('Error while serializing the result:')
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
        logger.exception('Error while finishing the activity:')
        return False
    return True


class SWFWorkflow(Task):
    def __init__(self, swf_client, input, token, running, timedout, results,
                 errors, spec, tags, rate_limit=64):
        self._swf_client = swf_client
        self._tags = tags
        self._spec = spec
        self._running = set(map(int, running))
        self._timedout = set(map(int, timedout))
        self._results = dict((int(k), v) for k, v in results.items())
        self._errors = dict((int(k), v) for k, v in errors.items())
        self._max_schedule = max(0, (rate_limit - len(self._running)))
        self._call_id = 0
        self._closed = False
        self._decisions = Layer1Decisions()
        super(SWFWorkflow, self).__init__(input, token)

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, tags=_sentinel):
        old_tags = self._tags
        if tags is not _sentinel:
            self._tags = tags
        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            yield
        self._tags = old_tags

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

    def fail(self, reason):
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
                return self.fail(e)
        # No need to cover this case - if it's a placeholder it must be
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
        return not self._running

    def schedule_activity(self, spec, input, retry, delay, default=None):
        return self._schedule(spec, input, retry, delay, default, True)

    def schedule_workflow(self, spec, input, retry, delay, default=None):
        return self._schedule(spec, input, retry, delay, default, False)

    def _schedule(self, spec, input, retry, delay, default=None, is_act=True):
        initial_call_id = self._call_id
        try:
            if delay:
                state, _ = self._search_timer()
                if state == _NOTFOUND:
                    self._schedule_timer(delay)
                    return default
                if state == _RUNNING:
                    return default
                assert state == _FOUND
            state, value = self._search_result(retry)
            if state == _RUNNING:
                return default
            if state == _ERROR:
                raise TaskError(value)
            if state == _FOUND:
                return value
            if state == _NOTFOUND:
                self._schedule_task(spec, is_act)
                return default
            if state == _TIMEDOUT:
                raise TaskTimedout()
        finally:
            self._reserve_call_ids(initial_call_id, delay, retry)

    def _search_timer(self):
        if self._call_id not in self._results:
            return _NOTFOUND, None
        self._call_id += 1
        if self._call_id in self._running:
            return _RUNNING, None
        return _FOUND, None

    def _schedule_timer(self, delay):
        if self._max_schedule > 0:
            self._decisions.start_timer(
                start_to_fire_timeout=str(delay),
                timer_id=str(self._call_id)
            )
            self._max_schedule -= 1

    def _search_result(self, retry):
        for self._call_id in range(self._call_id, self._call_id + retry + 1):
            if self._call_id in self._timedout:
                continue
            if self._call_id in self._running:
                return _RUNNING, None
            if self._call_id in self._errors:
                return _ERROR, self._errors[self._call_id]
            if self._call_id in self._results:
                return _FOUND, self._results[self._call_id]
            return _NOTFOUND, None
        raise _TIMEDOUT, None

    def _schedule_task(self, spec, is_act):
        if self._max_schedule > 0:
            call_id = self._call_id
            if not is_act:
                call_id = '%s-%s' % (uuid.uuid4(), self._call_id)
            spec.schedule(self._decisions, call_id, input)
            self._max_schedule -= 1

    def _reserve_call_ids(self, call_id, delay, retry):
        self._call_id = (
            1 + call_id         # one for the first call
            + int(delay > 0)    # one for the timer if needed
            + retry             # one for each possible retry
        )

    _serialize_restart_arguments = serialize_args
