import uuid

from boto.swf.exceptions import SWFResponseError
from boto.swf.layer1_decisions import Layer1Decisions

from flowy import str_or_none, logger, MagicBind
from flowy.result import Error, Placeholder, Result, Timeout


class AsyncActivityScheduler(object):
    def __init__(self, client):
        self._client = client

    def heartbeat(self, token):
        try:
            self._client.record_activity_task_heartbeat(task_token=token)
        except SWFResponseError:
            logger.exception('Error while sending the heartbeat:')
            return False
        return True

    def complete(self, token, result):
        try:
            self._client.respond_activity_task_completed(
                result=result, task_token=token
            )
        except SWFResponseError:
            logger.exception('Error while completing the activity:')
            return False
        return True

    def fail(self, token, reason):
        try:
            self._client.respond_activity_task_failed(
                reason=reason[:256], task_token=token
            )
        except SWFResponseError:
            logger.exception('Error while failing the activity:')
            return False
        return True

    def suspend(self, token):
        pass  # pragma: no cover


def ActivityScheduler(client, token):
    return MagicBind(AsyncActivityScheduler(client), token=token)


class DecisionScheduler(object):
    def __init__(self, client, token, running, timedout, results, errors,
                 rate_limit=64):
        self._client = client
        self._token = token
        self._running = running
        self._timedout = timedout
        self._results = results
        self._errors = errors
        self._call_id = 0
        self._decisions = Layer1Decisions()
        self._max_schedule = max(0, (rate_limit - len(self._running)))
        self._closed = False

    def remote_activity(self, task_id, input, result_deserializer,
                        heartbeat, schedule_to_close,
                        schedule_to_start, start_to_close,
                        task_list, retry, delay, error_handling):
        initial_call_id = self._call_id
        result = self._get_existing_result(
            delay, retry, result_deserializer, error_handling
        )
        if result is None:
            name, version = task_id
            if self._max_schedule > 0:
                self._decisions.schedule_activity_task(
                    str(self._call_id), name, version,
                    heartbeat_timeout=str_or_none(heartbeat),
                    schedule_to_close_timeout=str_or_none(schedule_to_close),
                    schedule_to_start_timeout=str_or_none(schedule_to_start),
                    start_to_close_timeout=str_or_none(start_to_close),
                    task_list=str_or_none(task_list),
                    input=str(input)
                )
                self._max_schedule -= 1
            result = Placeholder()
        self._reserve_call_ids(initial_call_id, delay, retry)
        return result

    def remote_subworkflow(self, task_id, input, result_deserializer,
                           workflow_duration, decision_duration,
                           task_list, retry, delay, error_handling):
        initial_call_id = self._call_id
        result = self._get_existing_result(
            delay, retry, result_deserializer, error_handling
        )
        if result is None:
            name, version = task_id
            subworkflow_id = '%s-%s' % (uuid.uuid4(), self._call_id)
            wf_duration = workflow_duration
            if self._max_schedule > 0:
                self._decisions.start_child_workflow_execution(
                    name, version, subworkflow_id,
                    execution_start_to_close_timeout=str_or_none(wf_duration),
                    task_start_to_close_timeout=str_or_none(decision_duration),
                    task_list=str_or_none(task_list),
                    input=str(input)
                )
                self._max_schedule -= 1
            result = Placeholder()
        self._reserve_call_ids(initial_call_id, delay, retry)
        return result

    def complete(self, result):
        if not self._running and not self._decisions._data:
            self._decisions.complete_workflow_execution(result=result)
        return self.suspend()

    def fail(self, reason):
        self._decisions = Layer1Decisions()
        self._decisions.fail_workflow_execution(reason=reason[:256])
        return self.suspend()

    def suspend(self):
        if self._closed:
            return False
        self._closed = True
        try:
            self._client.respond_decision_task_completed(
                task_token=self._token, decisions=self._decisions._data
            )
            return True
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')
            return False
        finally:
            self._decisions = Layer1Decisions()

    def _get_existing_result(self, delay, retry, result_deserializer,
                             error_handling):
        result = self._timer_result(delay)
        if result is not None:
            return result
        result = self._search_result(
            retry=retry,
            result_deserializer=result_deserializer,
            error_handling=error_handling
        )
        if result:
            return result

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
                if self._max_schedule > 0:
                    self._decisions.start_timer(
                        start_to_fire_timeout=str(delay),
                        timer_id=str(self._call_id)
                    )
                    self._max_schedule -= 1
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
        if error_handling:
            return Timeout()
        # XXX: Improve this msg
        self._decisions.fail_workflow_execution('A job has timed out.')
        return Placeholder()
