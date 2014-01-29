from boto.swf.exceptions import SWFResponseError

from flowy.result import Error, Placeholder, Result, Timeout


class ActivityRuntime(object):
    def __init__(self, client, token):
        self._client = client
        self._token = token

    def heartbeat(self):
        try:
            self._client.record_activity_task_heartbeat(token=self._token)
        except SWFResponseError:
            return False
        return True

    def complete(self, result):
        try:
            self._client.respond_activity_task_completed(
                result=result, token=self._token
            )
        except SWFResponseError:
            return False
        return True

    def fail(self, reason):
        try:
            self._client.respond_activity_task_failed(
                reason=reason, token=self._token
            )
        except SWFResponseError:
            return False
        return True

    def suspend(self):
        pass


class DecisionRuntime(object):
    def __init__(self, client, token, running, timedout, results, errors):
        self._client = client
        self._token = token
        self._running = running
        self._timedout = timedout
        self._results = results
        self._errors = errors
        self._call_id = 0
        self._is_completed = not running

    def remote_activity(self, name, version, args, kwargs, transport,
                        heartbeat=None,
                        schedule_to_close=None,
                        schedule_to_start=None,
                        start_to_close=None,
                        task_list=None,
                        retry=None,
                        delay=None,
                        error_handling=None):
        initial_call_id = self._call_id
        if (not self._deps_in_args(args, kwargs, error_handling) and
            not self._timer_not_done(delay) and
            not self._search_result(retry,
                                    transport.result_deserializer,
                                    error_handling)):
            self._is_completed = False
            raw_args, raw_kwargs = self._replace_results(args, kwargs)
            self._decision.queue_activity(  # XXX: implement
                call_id=str(self._call_id),
                name=name,
                version=version,
                input=transport.serialize_input(
                    *raw_args, **raw_kwargs
                ),
                heartbeat=heartbeat,
                schedule_to_close=schedule_to_close,
                schedule_to_start=schedule_to_start,
                start_to_close=start_to_close,
                task_list=task_list,
            )
            result = Placeholder()
        self._reserve_call_ids(initial_call_id, delay, retry)
        return result

    def remote_subworkflow(self, result_deserializer,
                           heartbeat=None,
                           workflow_duration=None,
                           decision_duration=None,
                           task_list=None,
                           retry=3,
                           delay=0,
                           error_handling=None):
        pass

    def complete(self, result):
        pass

    def fail(self, reason):
        pass

    def suspend(self):
        pass

    def _reserve_call_ids(self, call_id, delay, retry):
        self._call_id = (
            1 + call_id         # one for the first call
            + int(delay > 0)    # one for the timer if needed
            + retry             # one for each possible retry
        )

    def _deps_in_args(self, args, kwargs, error_handling):
        a = tuple(args) + tuple(kwargs.items())
        errs = list(filter(lambda x: isinstance(x, Error), a))
        if errs:
            composed_err = "\n".join(e._reason for e in errs)
            if error_handling:
                return Error(composed_err)
            else:
                self.fail(composed_err)
                return Placeholder()
        if any(isinstance(r, Placeholder) for r in a):
            return Placeholder()
        return False  # this means the args are ok

    def _timer_not_done(self, delay):
        if not delay:
            return True
        if self._call_id in self._running:
            return Placeholder()
        if self._call_id not in self._results:
            #XXX: queue timer
            return Placeholder()
        self._call_id += 1
        return False

    def _search_result(self, retry, result_deserializer, error_handling):
        for self._call_id in range(self._call_id, self._call_id + retry + 1):
            if self._call_id in self._timedout:
                continue
            if self._call_id in self._running:
                return Placeholder()
            if self._call_id in self._errors:
                error_message = self._errors[self._call_id]
                if error_handling:
                    return Error(error_message)
                self.fail(error_message)
                return Placeholder()
            if self._call_id in self._results:
                result = self._results[self._call_id]
                return Result(result_deserializer(result))
            return False  # There is nothing we could find about this call
        if self._error_handling:
            return Timeout()
        self._decision.fail('A job has timed out.')
        return Placeholder()

    def _replace_results(self, args, kwargs):
        raw_args = [
            arg.result() if isinstance(arg, Result) else arg for arg in args
        ]
        raw_kwargs = dict(
            (k, v.result() if isinstance(v, Result) else v)
            for k, v in kwargs.items()
        )
        return raw_args, raw_kwargs
