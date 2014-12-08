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
        self._scheduled = []
        self._flush = 1

    def abort(self, reason):
        self._flush = 0
        self._fail(reason)
        raise SuspendTask

    def _fail(self, reason):
        self._backend.fail(str(reason))

    def _flush(self):
        if not self._flush:
            return
        for proxy, call_key, a, kw, delay in self._scheduled:
            try:
                aa, kwkw = _extract_results(a, kw)
            except SuspendTask:
                # One of the .result() call failed, the workflow already failed
                return
            else:
                self._backend.schedule(proxy, call_key, aa, kwkw, delay)
        self._backend.flush()

    def _finish(self, r):
        if isinstance(r, restart):
            errs, placeholders = _short_circuit_on_args(r.a, r.kw)
            if errs:
                self._fail(first(errs))
            elif not placeholders:
                try:
                    aa, kwkw = _extract_results(a, kw)
                except SuspendTask:
                    # One of the .result() call failed the workflow already
                    return
                self._backend.restart(aa, kwkw)
        else:
            self._backend.complete(r)

    def _lookup(self, wf_bound_proxy):
        states = self._get_states()
        r = wf_bound_proxy(states)
        self._call_id += 1
        return r

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
                r = ResultWrapper(proxy.Result(result, order), self)
                break
            elif call_key in self._errors:
                raw_error = self._errors[call_key]
                order = self._order.index(call_key)
                r = proxy.Error(raw_error, order)
                break
            else:
                errs, placeholders = _short_circuit_on_args(a, kw)
                if errs:
                    r = proxy.LinkedError(first(errs))
                elif not placeholders:
                    self._schedule(proxy, call_key, a, kw, delay)
                break
        else:
            order = self._order.index(call_key)
            r = proxy.Timeout(order)
        self._call_id += 1
        return r

    def _schedule(self, proxy, call_key, a, kw, delay):
        t = len(self._scheduled) + len(self._running)
        if t < self.rate_limit or max(self.rate_limit, 0) == 0:
            self._scheduled.append(proxy, call_key, a, kw, delay)


def _short_circuit_on_args(a, kw):
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


def _extract_results(a, kw):
    aa = (_result_or_value(r) for r in a)
    kwkw = dict((k, _result_or_value(v)) for k, v in kw.iteritems())
    return aa, kwkw


def _result_or_value(r):
    if isinstance(r, TaskResult):
        return r.result()
    return r


def wait_for(result, *results):
    i = _i_or_args(result, results)
    for r in i:
        r.wait()


def first(result, *results):
    return min(_i_or_args(result, results)).wait()


def first_n(n, result, *results):
    i = _i_or_args(result, results)
    if n == 1:
        yield first(i)
        return
    s = sorted(i)
    for r in s[:n]:
        yield r.wait()


def all(result, *results):
    i = list(_i_or_args(result, results))
    for r in first_n(len(i), i):
        yield r


class ResultWrapper(object):
    def __init__(self, result, workflow):
        self._r = result
        self._workflow = workflow

    def result(self):
        try:
            return self._r.result()
        except Exception as e:
            logger.exception("Error when loading result:")
            self._workflow.abort(e)  # This will suspend the execution

    def __lt__(self, other):
        return self._r < other

    def wait(self):
        return self._r.wait()

    def is_error(self):
        return self._r.is_error()
