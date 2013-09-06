from pyswf.transport import JSONArgsTransport, JSONResultTransport
from pyswf.activity import ActivityError
from pyswf.datatype import DecisionTask, ActivityTask


class WorkflowContext(object):

    args_transport = JSONArgsTransport()
    result_transport = JSONResultTransport()

    def __init__(self, decision_task):
        self.decision_task = decision_task

    def is_empty(self):
        return self.decision_task.is_empty_response()

    @property
    def name(self):
        return self.decision_task.name

    @property
    def version(self):
        return self.decision_task.version

    @property
    def token(self):
        return self.decision_task.token

    @property
    def args(self):
        args, kwargs = self.args_transport.decode(self.decision_task.input)
        return args

    @property
    def kwargs(self):
        args, kwargs = self.args_transport.decode(self.decision_task.input)
        return kwargs

    def encode_args_kwargs(self, args, kwargs):
        return self.args_transport.encode(args, kwargs)

    def get_execution_state(self):
        return WorkflowExecutionState(self.decision_task)

    def any_activity_still_running(self):
        for sa in self.decision_task.scheduled_activities:
            eid = sa.event_id
            a1 = self.decision_task.completed_activity_by_scheduled_id(eid)
            a2 = self.decision_task.timedout_activity_by_scheduled_id(eid)
            if a1 is None and a2 is None:
                return True
        return False

    def execute(self, runner):
        runner_instance = runner(self.get_execution_state())
        # XXX: What happens when the workflow throws exceptions?
        scheduled_activities, result = runner_instance.invoke(
            *self.args, **self.kwargs
        )
        scheduled = []
        for invocation_id, activity, args, kwargs in scheduled_activities:
            input = self.encode_args_kwargs(args, kwargs)
            scheduled.append((invocation_id, activity, input))
        result = self.result_transport.encode_result(result)
        return scheduled, self.any_activity_still_running(), result


class WorkflowExecutionState(object):

    result_transport = JSONResultTransport()

    def __init__(self, decision_task):
        self.decision_task = decision_task

    def is_scheduled(self, invocation_id):
        a = self.decision_task.completed_activity_by_activity_id(invocation_id)
        return a is not None

    def result_value(self, result):
        return self.result_transport.value(result)

    def is_result_error(self, result):
        return self.result_transport.is_error(result)

    def result_for(self, invocation_id, default=None):
        a = self.decision_task.completed_activity_by_activity_id(invocation_id)
        if a is None:
            return default
        return self.result_value(a.result)

    def is_error(self, invocation_id):
        a = self.decision_task.completed_activity_by_activity_id(invocation_id)
        if a is None:
            return False
        return self.is_result_error(a.result)

    def is_timedout(self, invocation_id):
        a = self.decision_task.timedout_activity_by_activity_id(invocation_id)
        return a is not None


class ActivityContext(object):

    args_transport = JSONArgsTransport()
    result_transport = JSONResultTransport()

    def __init__(self, activity_task):
        self.activity_task = activity_task

    def is_empty(self):
        return self.activity_task.is_empty_response()

    @property
    def name(self):
        return self.activity_task.name

    @property
    def version(self):
        return self.activity_task.version

    @property
    def token(self):
        return self.activity_task.token

    @property
    def args(self):
        args, kwargs = self.args_transport.decode(self.activity_task.input)
        return args

    @property
    def kwargs(self):
        args, kwargs = self.args_transport.decode(self.activity_task.input)
        return kwargs

    def encode_result(self, result):
        return self.result_transport.encode(result)

    def execute(self, runner):
        try:
            result = runner.invoke(*self.args, **self.kwargs)
            return self.result_transport.encode_result(result)
        except ActivityError as e:
            return self.result_transport.encode_error(e.message)
