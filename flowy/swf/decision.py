import uuid

from boto.exception import SWFResponseError
from boto.swf.layer1_decisions import Layer1Decisions

from flowy.utils import logger


INPUT_SIZE = RESULT_SIZE = 32768
REASON_SIZE = 256


class SWFActivityDecision(object):
    def __init__(self, layer1, token):
        self.layer1 = layer1
        self.token = token

    def heartbeat(self):
        try:
            self.layer1.record_activity_task_heartbeat(task_token=str(self.token))
        except SWFResponseError:
            logger.exception('Error while sending the heartbeat:')
            return False
        return True

    def fail(self, reason):
        try:
            self.layer1.respond_activity_task_failed(
                reason=str(reason)[:256], task_token=str(self.token))
        except SWFResponseError:
            logger.exception('Error while failing the activity:')
            return False
        return True

    def flush(self):
        self.fail("Cannot flush activities.")

    def restart(self, input_data):
        self.fail("Cannot restart activities.")

    def finish(self, result):
        result = str(result)
        if len(result) > RESULT_SIZE:
            self.fail("Result too large: %s/%s" % (len(result), RESULT_SIZE))
        try:
            self.layer1.respond_activity_task_completed(
                result=result, task_token=str(self.token))
        except SWFResponseError:
            logger.exception('Error while finishing the activity:')
            return False
        return True


class SWFWorkflowDecision(object):
    def __init__(self, layer1, token, name, version, task_list,
                 decision_duration, workflow_duration, tags, child_policy):
        self.layer1 = layer1
        self.token = token
        self.task_list = task_list
        self.decision_duration = decision_duration
        self.workflow_duration = workflow_duration
        self.tags = tags
        self.child_policy = child_policy
        self.decisions = Layer1Decisions()
        self.closed = False

    def fail(self, reason):
        """Fail the workflow and flush.

        Any other decisions queued are cleared.
        The reason is truncated if too large.
        """
        decisions = self.decisions = Layer1Decisions()
        decisions.fail_workflow_execution(reason=str(reason)[:REASON_SIZE])
        self.flush()

    def flush(self):
        """Flush the decisions; no other decisions can be sent after that."""
        if self.closed:
            return
        self.closed = True
        try:
            self.layer1.respond_decision_task_completed(
                task_token=str(self.token), decisions=self.decisions._data)
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')
            # ignore the error and let the decision timeout and retry

    def restart(self, input_data):
        """Restart the workflow and flush.

        Any other decisions queued are cleared.
        """
        decisions = self.decisions = Layer1Decisions()
        input_data = str(input_data)
        if len(input_data) > INPUT_SIZE:
            self.fail("Restart input too large: %s/%s" % (len(input_data), INPUT_SIZE))
        else:
            decisions.continue_as_new_workflow_execution(
                start_to_close_timeout=self.decision_duration,
                execution_start_to_close_timeout=self.workflow_duration,
                task_list=self.task_list,
                input=input_data,
                tag_list=self.tags,
                child_policy=self.child_policy)
        self.flush()

    def finish(self, result):
        """Finish the workflow execution and flush.

        Any other decisions queued are cleared.
        """
        decisions = self.decisions = Layer1Decisions()
        result = str(result)
        if len(result) > RESULT_SIZE:
            self.fail("Result too large: %s/%s" % (len(result), RESULT_SIZE))
        else:
            decisions.complete_workflow_execution(result)
            self.flush()

    def schedule_timer(self, call_key, delay):
        """Schedule a timer. This is used to delay execution of tasks."""
        self.decisions.start_timer(timer_id=timer_key(call_key),
                                   start_to_fire_timeout=str(delay))

    def schedule_activity(self, call_key, name, version, input_data, task_list,
                          heartbeat, schedule_to_close, schedule_to_start,
                          start_to_close):
        """Schedule an activity execution."""
        input_data = str(input_data)
        if len(input_data) > INPUT_SIZE:
            self.fail("Activity input too large: %s/%s" % (len(input_data), INPUT_SIZE))
        self.decisions.schedule_activity_task(
            call_key, name, version,
            heartbeat_timeout=heartbeat,
            schedule_to_close_timeout=schedule_to_close,
            schedule_to_start_timeout=schedule_to_start,
            start_to_close_timeout=start_to_close,
            task_list=task_list,
            input=input_data)

    def schedule_workflow(self, call_key, name, version, input_data, task_list,
                          workflow_duration, decision_duration, child_policy):
        """Schedule a workflow execution."""
        input_data = str(input_data)
        if len(input_data) > INPUT_SIZE:
            self.fail("Workflow input too large: %s/%s" % (len(input_data), INPUT_SIZE))
        call_key = '%s:%s' % (uuid.uuid4(), call_key)
        self.decisions.start_child_workflow_execution(
            name, version, call_key,
            task_start_to_close_timeout=decision_duration,
            execution_start_to_close_timeout=workflow_duration,
            task_list=task_list,
            input=input_data,
            child_policy=child_policy)


class SWFWorkflowTaskDecision(object):
    def __init__(self, decision, execution_history, proxy_factory, rate_limit):
        self.decision = decision
        self.execution_history = execution_history
        self.proxy_factory = proxy_factory
        self.rate_limit = rate_limit

    def fail(self, reason):
        self.decision.fail(reason)

    def schedule(self, call_number, retry_number, delay, input_data):
        if not self.rate_limit.consume():
            return
        tk = task_key(self.proxy_factory.identity, call_number, retry_number)
        if delay > 0:
            if self.execution_history.is_timer_ready(tk):
                self._schedule(tk, input_data)
            elif not self.execution_history.is_timer_running(tk):
                self.decision.schedule_timer(tk, delay)
        else:
            self._schedule(tk, input_data)

    def _schedule(self, task_key, input_data):
        self.decision.schedule_workflow(
            task_key, self.proxy_factory.name, self.proxy_factory.version, input_data,
            self.proxy_factory.task_list, self.proxy_factory.workflow_duration,
            self.proxy_factory.decision_duration, self.proxy_factory.child_policy)


class SWFActivityTaskDecision(SWFWorkflowTaskDecision):
    def _schedule(self, task_key, input_data):
        self.decision.schedule_activity(
            task_key, self.proxy_factory.name, self.proxy_factory.version, input_data,
            self.proxy_factory.task_list, self.proxy_factory.heartbeat,
            self.proxy_factory.schedule_to_close, self.proxy_factory.schedule_to_start,
            self.proxy_factory.start_to_close)




def timer_key(call_key):
    return '%s:t' % call_key


def task_key(identity, call_number, retry_number):
    return '%s-%s-%s' % (identity, call_number, retry_number)
