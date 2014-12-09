import json
import logging
import uuid
from contextlib import contextmanager

from boto.swf.exceptions import SWFResponseError
from boto.swf.layer1_decisions import Layer1Decisions
from flowy.exception import SuspendTask, TaskError
from flowy.result import Error, Result, Timeout
from flowy.spec import _sentinel


logger = logging.getLogger(__name__)


serialize_result = staticmethod(json.dumps)
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


class _SWFWorkflow(Task):

    _TIMEDOUT, _RUNNING, _ERROR, _FOUND, _NOTFOUND = range(5)

    def __init__(self, scheduler, input, token, running, timedout, results,
                 errors, order, spec, tags):
        self._scheduler = scheduler
        self._running = set(map(int, running))
        self._timedout = set(map(int, timedout))
        self._results = dict((int(k), v) for k, v in results.items())
        self._errors = dict((int(k), v) for k, v in errors.items())
        self._order = list(map(int, order))
        self._spec = spec
        self._tags = tags
        self._scheduled = False
        self._call_id = 0
        super(_SWFWorkflow, self).__init__(input, token)

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

    def first_result(self, *results):
            return min(results).result()

    def first_results(self, n, *results):
        n = int(n)
        if not len(results) > 1:
            raise ValueError("No results to wait for.")
        if n == 1:
            return self.first_result(*results)
        elif n < len(results):
            return [r.result() for r in sorted(results)[:n]]
        else:
            return all_results(*results)

    def all_results(self, *results):
        return [r.result() for r in results]

    def restart(self, *args, **kwargs):
        try:
            input = self._serialize_restart_arguments(*args, **kwargs)
        except TypeError:
            logger.exception('Error while serializing restart arguments:')
            return False
        self._scheduled = True
        return self._scheduler.restart(self._spec, input, self._tags)

    def fail(self, reason):
        self._scheduled = True
        return self._scheduler.fail(reason)

    def _suspend(self):
        return self._scheduler.flush()

    def _finish(self, result):
        r = result
        if isinstance(result, Result):
            r = result.result()
        elif isinstance(result, (Error, Timeout)):
            try:
                result.result()
            except TaskError as e:
                return self._scheduler.fail(e)
        if not self._scheduled and not self._running:
            try:
                r = self._serialize_result(r)
            except TypeError:
                logger.exception("Error while serializing the result:")
                return False
            return self._scheduler.complete(r)
        return self._scheduler.flush()

    def schedule_activity(self, spec, input, retry, delay):
        return self._schedule(spec, input, retry, delay, True)

    def schedule_workflow(self, spec, input, retry, delay):
        return self._schedule(spec, input, retry, delay, False)

    def _schedule(self, spec, input, retry, delay, is_act=True):
        initial_call_id = self._call_id
        try:
            if delay:
                state = self._search_timer()
                if state == self._NOTFOUND:
                    self._scheduled = True
                    self._scheduler.schedule_timer(delay, self._call_id)
                    state = self._RUNNING
                if not(state == self._FOUND):
                    return state, None, None
            state, value, order = self._search_result(retry)
            if state == self._NOTFOUND:
                self._scheduled = True
                sched = self._scheduler.schedule_activity
                if not is_act:
                    sched = self._scheduler.schedule_workflow
                sched(spec, self._call_id, input)
                return self._RUNNING, None, None
            return state, value, order
        finally:
            self._reserve_call_ids(initial_call_id, delay, retry)

    def _search_timer(self):
        if self._call_id in self._results:
            self._call_id += 1
            return self._FOUND
        if self._call_id in self._running:
            return self._RUNNING
        return self._NOTFOUND

    def _search_result(self, retry):
        # update self._call_id automatically
        for self._call_id in range(self._call_id, self._call_id + retry + 1):
            if self._call_id in self._timedout:
                continue
            if self._call_id in self._running:
                return self._RUNNING, None, None
            if self._call_id in self._errors:
                return (self._ERROR,
                        self._errors[self._call_id],
                        self._order.index(self._call_id))
            if self._call_id in self._results:
                return (self._FOUND,
                        self._results[self._call_id],
                        self._order.index(self._call_id))
            return self._NOTFOUND, None, None
        return self._TIMEDOUT, None, self._order.index(self._call_id)

    def _reserve_call_ids(self, call_id, delay, retry):
        self._call_id = (
            1 + call_id         # one for the first call
            + int(delay > 0)    # one for the timer if needed
            + retry             # one for each possible retry
        )

    _serialize_restart_arguments = serialize_args


# It's important for the scheduler to ignore anything after the first flush
# since the task doesn't promise calling it only once
class SWFScheduler(object):
    def __init__(self, swf_client, token, rate_limit=64):
        self._swf_client = swf_client
        self._token = token
        self._rate_limit = rate_limit
        self._decisions = Layer1Decisions()
        self._closed = False

    def flush(self):
        if self._closed:
            return False
        self._closed = True
        try:
            self._swf_client.respond_decision_task_completed(
                task_token=self._token, decisions=self._decisions._data
            )
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')
            return False
        return True

    def restart(self, spec, input, tags):
        decisions = self._decisions = Layer1Decisions()
        spec.restart(decisions, input, tags)
        return self.flush()

    def fail(self, reason):
        decisions = self._decisions = Layer1Decisions()
        decisions.fail_workflow_execution(reason=str(reason)[:256])
        return self.flush()

    def complete(self, result):
        decisions = self._decisions = Layer1Decisions()
        decisions.complete_workflow_execution(result)
        return self.flush()

    def schedule_timer(self, delay, call_id):
        if len(self._decisions._data) < self._rate_limit:
            self._decisions.start_timer(
                start_to_fire_timeout=str(delay),
                timer_id=str(call_id)
            )

    def schedule_activity(self, spec, call_id, input):
        if len(self._decisions._data) < self._rate_limit:
            spec.schedule(self._decisions, call_id, input)

    def schedule_workflow(self, spec, call_id, input):
        if len(self._decisions._data) < self._rate_limit:
            call_id = '%s-%s' % (uuid.uuid4(), call_id)
            spec.schedule(self._decisions, call_id, input)


class SWFWorkflow(_SWFWorkflow):
    def __init__(self, swf_client, input, token, running, timedout, results,
                 errors, order, spec, tags):
        s = SWFScheduler(swf_client, token, rate_limit=64 - len(running))
        super(SWFWorkflow, self).__init__(s, input, token, running, timedout,
                                          results, errors, order, spec, tags)
