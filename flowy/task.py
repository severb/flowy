import json
import logging

from flowy.exception import SuspendTask
from flowy.result import TaskResult


logger = logging.getLogger(__package__)


class Task(object):
    def __call__(self, *args, **kwargs):
        try:
            result = self.run(*args, **kwargs)
        except SuspendTask:
            self._flush()
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


class restart(object):
    def __init__(self, *args, **kwargs):
        self.a, self.kw = args, kwargs


class Workflow(Task):

    rate_limit = 64

    def __init__(self, backend, running, timedout, results, errs, ordr):
        self._backend = backend
        self._running = set(running)
        self._timedout = set(timedout)
        self._results = dict(results)
        self._errors = dict(errs)
        self._order = list(ordr)
        self._call_id = 0
        self._scheduled = 0

    def wait_for(self, result, *results):
        i = _i_or_args(result, results)
        for r in i:
            r.wait()

    def first(self, result, *results):
        return min(_i_or_args(result, results)).wait()

    def first_n(self, n, result, *results):
        i = _i_or_args(result, results)
        if n == 1:
            yield self.first(i)
            return
        s = sorted(i)
        for r in s[:n]:
            yield r.wait()

    def all(self, result, *results):
        i = list(_i_or_args(result, results))
        for r in self.first_n(len(i), i):
            yield r

    def _fail(self, reason):
        self._backend.fail(str(reason))

    def _flush(self):
        self._backend.flush()

    def _finish(self, r):
        if isinstance(r, restart):
            errs, placeholders = self._short_circuit_on_args(r.a, r.kw)
            if errs:
                self._fail(self.first(errs))
            elif not placeholders:
                aa, kwkw = self._extract_results(a, kw)
                self._backend.restart(aa, kwkw)
        else:
            self._backend.complete(r)

    def _lookup(self, proxy, a, kw):
        r = proxy.Placeholder()
        for call_number, delay in enumerate(proxy):
            call_key = "%s-%s" % (self._call_id, call_number)
            if call_key in self._timedout:
                continue
            elif call_key in self._running:
                break
            elif call_key in self._results:
                result = self._results[call_key]
                order = self._order.index(call_key)
                r = proxy.Result(result, order)
                break
            elif call_key in self._errors:
                raw_error = self._errors[call_key]
                order = self._order.index(call_key)
                r = proxy.Error(raw_error, order)
                break
            else:
                errs, placeholders = self._short_circuit_on_args(a, kw)
                if errs:
                    r = proxy.LinkedError(self.first(errs))
                elif not placeholders:
                    self._schedule(proxy, call_key, a, kw, delay)
                break
        else:
            order = self._order.index(call_key)
            r = proxy.Timeout(order)
        self._call_id += 1
        return r

    def _schedule(self, proxy, call_key, a, kw, delay):
        t = self._scheduled + len(self._running)
        if t < self.rate_limit or max(self.rate_limit, 0) == 0:
            proxy.schedule(self._backend, call_key, a, kw, delay)
            self._scheduled += 1

    def _extract_results(self, a, kw):
        aa = (self._result_or_value(r) for r in a)
        kwkw = dict((k, self._result_or_value(v)) for k, v in kw.iteritems())
        return aa, kwkw

    def _result_or_value(self, r):
        if isinstance(r, TaskResult):
            try:
                return r.result()
            except Exception as e:
                logger.exception("Error while reading result:")
                raise e  # Let it bubble up and stop the workflow

    def _short_circuit_on_args(self, a, kw):
        args = a + tuple(kw.values())
        errs, placeholders = [], False
        for r in args:
            if isinstance(r, TaskResult):
                try:
                    if r.is_error():
                        errs.append(r)
                except SuspendTask:
                    placeholders = True
        return errs, placeholders


def _i_or_args(result, results):
    if len(results) == 0:
        return iter(result)
    return (result,) + results
