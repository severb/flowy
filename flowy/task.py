import json
import logging
import uuid
from contextlib import contextmanager

from boto.swf.exceptions import SWFResponseError
from boto.swf.layer1_decisions import Layer1Decisions

from flowy.exception import SuspendTask
from flowy.exception import TaskError
from flowy.proxy import serialize_arguments
from flowy.result import Error
from flowy.result import LinkedError
from flowy.result import Placeholder
from flowy.result import Result
from flowy.result import Timeout
from flowy.spec import _sentinel

try:
    from itertools import izip
except ImportError:
    izip = zip


logger = logging.getLogger(__name__)


def serialize_result(result):
    r = json.dumps(result)
    if len(r) > 32000:
        raise ValueError("Serialized result > 32000 characters.")
    return r


class Task(object):
    def __init__(self, input):
        self._input = input

    def __call__(self):
        try:
            args, kwargs = self._deserialize_arguments(self._input)
        except Exception as e:
            logger.exception("Error while deserializing the arguments:")
            self._fail(e)
        else:
            try:
                result = self.run(*args, **kwargs)
            except SuspendTask:
                pass
            except Exception as e:
                logger.exception("Error while running the task:")
                self._fail(e)
            else:
                self._finish(result)
        self._flush()

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def _flush(self):
        raise NotImplementedError

    def _fail(self, reason):
        raise NotImplementedError

    def _finish(self, result):
        raise NotImplementedError

    _serialize_result = staticmethod(serialize_result)
    _deserialize_arguments = staticmethod(json.loads)


def activity_fail(self, reason):
    try:
        self._swf_client.respond_activity_task_failed(
            reason=str(reason)[:256], task_token=str(self._token))
    except SWFResponseError:
        logger.exception('Error while failing the activity:')
        return False
    return True


def activity_finish(self, result):
    try:
        result = self._serialize_result(result)
    except Exception as e:
        logger.exception('Error while serializing the result:')
        return activity_fail(self, e)
    try:
        self._swf_client.respond_activity_task_completed(
            result=str(result), task_token=str(self._token))
    except SWFResponseError:
        logger.exception('Error while finishing the activity:')
        return False
    return True


def activity_heartbeat(self):
    try:
        t = str(self._token)
        self._swf_client.record_activity_task_heartbeat(task_token=t)
    except SWFResponseError:
        logger.exception('Error while sending the heartbeat:')
        return False
    return True


class AsyncSWFActivity(object):

    heartbeat = activity_heartbeat
    fail = activity_fail
    finish = activity_finish
    _serialize_result = staticmethod(serialize_result)

    def __init__(self, swf_client, token):
        self._swf_client = swf_client
        self._token = token


class SWFActivity(Task):

    heartbeat = activity_heartbeat
    _fail = activity_fail
    _finish = activity_finish

    def __init__(self, swf_client, input, token):
        self._swf_client = swf_client
        self._token = token
        super(SWFActivity, self).__init__(input, token)

    def _flush(self):
        pass


class Workflow(Task):

    def __init__(self, scheduler, input, running, timedout, results, errors,
                 order, spec):
        self._scheduler = scheduler
        self._running = set(running)
        self._timedout = set(timedout)
        self._results = dict(results)
        self._errors = dict(errors)
        self._order = list(order)
        self._spec = spec
        self._call_id = 0
        super(Workflow, self).__init__(input)

    def wait_for(self, task):
        if isinstance(task, Placeholder):
            raise SuspendTask
        return task

    def first(self, result, *results):
        return self.wait_for(min(_i_or_args(result, results)))

    def first_n(self, n, result, *results):
        i = _i_or_args(result, results)
        if n == 1:
            yield self.first(i)
            return
        s = sorted(i)
        for r in s[:n]:
            yield self.wait_for(r)

    def all(self, result, *results):
        i = list(_i_or_args(result, results))
        for r in self.first_n(len(i), i):
            yield r

    def restart(self, *args, **kwargs):
        try:
            input = self._serialize_restart_arguments(*args, **kwargs)
        except Exception as e:
            logger.exception('Error while serializing restart arguments:')
            self._fail(e)
        else:
            self._scheduler.reset()
            self._scheduler.restart(self._spec, input)
        raise SuspendTask

    def _fail(self, reason):
        self._scheduler.reset()
        self._scheduler.fail(reason)

    def _finish(self, result):
        if isinstance(result, Placeholder):
            return
        r = result
        if isinstance(result, Result):
            try:
                r = result.result()
            except TaskError as e:
                self._fail(e)
                return
        try:
            r = self._serialize_result(r)
        except Exception as e:
            logger.exception("Error while serializing the result:")
            self._fail(e)
            return
        self._scheduler.reset()
        self._scheduler.complete(r)

    def _flush(self):
        self._scheduler.flush()

    def _schedule(self, proxy, a, kw, retry, d_result):
        s = self._scheduler.schedule
        return self._sched(proxy, a, kw, retry, d_result,
                           self._fail_execution, self._fail_on_result, s,
                           self._fail_execution)

    def _schedule_with_err(self, proxy, a, kw, retry, d_result):
        s = self._scheduler.schedule
        return self._schedule(proxy, a, kw, retry, d_result, Error,
                              LinkedError, s, Timeout)

    def _sched(self, spec, a, kw, retry, d_result, fail_task,
                  fail_on_result, schedule, timeout):
        r = Placeholder()
        for call_number, delay in enumerate(retry):
            call_key = "%s-%s" % (self._call_id, call_number)
            if call_key in self._timedout:
                continue
            elif call_key in self._running:
                break
            elif call_key in self._results:
                raw_result = self._results[call_key]
                order = self._order.index(call_key)
                r = Result(raw_result, d_result, order)
                break
            elif call_key in self._errors:
                raw_error = self._errors[call_key]
                order = self._order.index(call_key)
                r = fail_task(raw_error, order)
                break
            else:
                args = a + tuple(kw.values())
                # Optimization not to schedule or load results on errors
                errs = [x for x in args if isinstance(x, Error)]
                print '-'*80
                print errs, args
                print '-'*80
                if errs:
                    r = fail_on_result(min(errs))
                    break
                # Favor result loading vs activity schedule
                errs, aa, kwkw = self._solve_args(a, kw)
                if errs:
                    r = fail_on_result(min(errs))
                    break
                if any(isinstance(x, Placeholder) for x in args):
                    break
                try:
                    schedule(spec, call_key, aa, kwkw, delay)
                except Exception as e:
                    logger.exception("Error while scheduling task:")
                    r = fail_task(e, -1)
                break
        else:
            order = self._order.index(call_key)
            # XXX: Improve this error message.
            r = timeout("A task has timedout.", order)
        self._call_id += 1
        return r

    def _fail_execution(self, reason, order=None):
        self._fail(reason)
        raise SuspendTask

    def _fail_on_result(self, err, suspend=True):
        try:
            err.result()
        except TaskError as e:
            return self._fail_execution(e)

    def _solve_args(self, a, kw):
        errs = []
        aa = []
        for x in a:
            if isinstance(x, Result):
                try:
                    aa.append(x.result())
                except TaskError:
                    errs.append(x)
            else:
                aa.append(x)
        kwkw = {}
        for k, v in kw:
            if isinstance(v, Result):
                try:
                    kwkw[k] = v.result()
                except TaskError:
                    errs.append(v)
            else:
                kwkw[k] = v
        return errs, aa, kwkw

    _serialize_restart_arguments = staticmethod(serialize_arguments)


class SWFScheduler(object):
    def __init__(self, swf_client, token, rate_limit=64):
        self._swf_client = swf_client
        self._token = token
        self._rate_limit = rate_limit
        self._decisions = Layer1Decisions()
        self._closed = False

    def flush(self):
        if self._closed:
            raise RuntimeError('The scheduler is already flushed.')
        self._closed = True
        try:
            self._swf_client.respond_decision_task_completed(
                task_token=self._token, decisions=self._decisions._data
            )
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')

    def reset(self):
        self._decisions = Layer1Decisions()

    def restart(self, spec, input, tags):
        spec.restart(self._decisions, input, tags)

    def fail(self, reason):
        decisions.fail_workflow_execution(reason=str(reason)[:256])

    def complete(self, result):
        self._decisions.complete_workflow_execution(str(result))

    def schedule(self, proxy, call_key, a, kw, delay):
        if len(self._decisions._data) > self._rate_limit:
            return
        delay = int(delay)
        if max(delay, 0):
            self._decisions.start_timer(
                start_to_fire_timeout=str(delay),
                timer_id=str('%s:timer' % call_ke)
            )
        else:
            proxy.schedule(self._decisions, call_key, a, kw)


class SWFWorkflow(Workflow):
    def __init__(self, swf_client, input, token, running, timedout, results,
                 errors, order, spec, tags):
        s = SWFScheduler(swf_client, token, rate_limit=64 - len(running))
        self._tags = tags
        super(SWFWorkflow, self).__init__(s, input, running, timedout,
                                          results, errors, order, spec)

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
        except Exception as e:
            logger.exception('Error while serializing restart arguments:')
            self._fail(e)
        else:
            self._scheduler.reset()
            self._scheduler.restart(self._spec, input, self._tags)
        raise SuspendTask


def _i_or_args(result, results):
    if len(results) == 0:
        return iter(result)
    return (result,) + results
