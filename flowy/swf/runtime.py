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

    def remote_activity(self, name, version, input, result_deserializer,
                        heartbeat, schedule_to_close,
                        schedule_to_start, start_to_close,
                        task_list, retry, delay, error_handling):
        initial_call_id = self._call_id
        result = self._timer_result(delay)
        if result is not None:
            return result
        result = self._search_result(
            retry=retry,
            result_deserializer=result_deserializer,
            error_handling=error_handling
        )
        if result is not None:
            return result
        self._is_completed = False
        self._decision.queue_activity(  # XXX: implement
            call_id=self._call_id,
            name=name,
            version=version,
            input=input,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_list=task_list,
        )
        self._reserve_call_ids(initial_call_id, delay, retry)
        return Placeholder()

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

    def _timer_result(self, delay):
        if delay:
            if self._call_id in self._running:
                return Placeholder()
            if self._call_id not in self._results:
                #XXX: queue timer
                return Placeholder()
            self._call_id += 1

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
        self._decision.fail('A job has timed out.')  # XXX: Improve this msg
        return Placeholder()
