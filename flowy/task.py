import json
import logging
from contextlib import contextmanager

from flowy.exception import SuspendTask
from flowy.exception import SuspendTaskNoFlush
from flowy.exception import TaskError
from flowy.proxy import TaskProxy
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


class Task(object):
    def __init__(self, input):
        self._input = input

    def __call__(self):
        try:
            args, kwargs = self.deserialize_arguments(self._input)
        except Exception as e:
            logger.exception("Error while deserializing the arguments:")
            self._fail(e)
        else:
            try:
                result = self.run(*args, **kwargs)
            except SuspendTask:
                self._flush()
            except SuspendTaskNoFlush:
                pass
            except Exception as e:
                logger.exception("Error while running the task:")
                self._fail(e)
            else:
                self._finish(result)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def _flush(self):
        raise NotImplementedError

    def _fail(self, reason):
        raise NotImplementedError

    def _finish(self, result):
        raise NotImplementedError

    deserialize_arguments = staticmethod(json.loads)

    @staticmethod
    def serialize_result(result):
        r = json.dumps(result)
        if len(r) > 32000:
            raise ValueError("Serialized result > 32000 characters.")
        return r


class restart(object):
    def __init__(self, *args, **kwargs):
        self.a, self.kw = args, kwargs


class Workflow(Task):

    rate_limit = 64

    def __init__(self, input, running, timedout, results, errors, order):
        self._running = set(running)
        self._timedout = set(timedout)
        self._results = dict(results)
        self._errors = dict(errors)
        self._order = list(order)
        self._call_id = 0
        self._scheduled = []
        super(Workflow, self).__init__(input)

    def wait_for(self, task):
        # Don't force result deserialization
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

    def _restart(self, input):
        raise NotImplementedError

    def _complete(self, result):
        raise NotImplementedError

    def _fail(self, reason):
        raise NotImplementedError

    def _flush(self):
        raise NotImplementedError

    def _schedule(self, proxy, call_key, a, kw, delay):
        if len(self._scheduled) >= self.rate_limit - len(self._running):
            return
        self._scheduled.append((proxy, call_key, a, kw, delay))

    def _finish(self, r):
        if isinstance(r, restart):
            try:
                input = self._serialize_restart_arguments(*r.a, **r.kw)
            except Exception as e:
                logger.exception('Error while serializing restart arguments:')
                return self._fail(e)
            else:
                return self._restart(input)
        if isinstance(r, Placeholder):
            return self._flush()
        if isinstance(r, Result):
            try:
                r = r.result()
            except TaskError as e:
                return self._fail(e)
        try:
            r = self.serialize_result(r)
        except Exception as e:
            logger.exception("Error while serializing the result:")
            return self._fail(e)
        self._complete(r)

    def _lookup(self, proxy, a, kw):
        return self._l(proxy, a, kw, self._fail_execution,
                       self._fail_on_result, self._fail_execution)

    def _lookup_with_errors(self, proxy, a, kw):
        return self._l(proxy, a, kw, Error, LinkedError, Timeout)

    def _l(self, proxy, a, kw, fail_task, fail_on_result, timeout):
        r = Placeholder()
        for call_number, delay in enumerate(proxy):
            call_key = "%s-%s" % (self._call_id, call_number)
            if call_key in self._timedout:
                continue
            elif call_key in self._running:
                break
            elif call_key in self._results:
                raw_result = self._results[call_key]
                order = self._order.index(call_key)
                r = Result(raw_result, proxy.deserialize_result, order)
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
                    self._schedule(proxy, call_key, aa, kwkw, delay)
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
        raise SuspendTaskNoFlush

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

    _serialize_restart_arguments = staticmethod(TaskProxy.serialize_arguments)


def _i_or_args(result, results):
    if len(results) == 0:
        return iter(result)
    return (result,) + results
