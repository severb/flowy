from flowy.swf.decision import SWFActivityTaskDecision
from flowy.swf.decision import SWFWorkflowTaskDecision
from flowy.swf.history import SWFTaskExecutionHistory
from flowy.proxy import Proxy
from flowy.utils import DescCounter


class SWFActivityProxyFactory(object):
    """A proxy factory for activities."""

    def __init__(self, identity, name, version,
                 task_list=None,
                 heartbeat=None,
                 schedule_to_close=None,
                 schedule_to_start=None,
                 start_to_close=None,
                 retry=(0, 0, 0),
                 serialize_input=None,
                 deserialize_result=None):
        # This is a unique name used to generate unique identifiers
        self.identity = identity
        self.name = name
        self.version = version
        self.task_list = task_list
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.start_to_close = start_to_close
        self.retry = retry
        self.serialize_input = serialize_input
        self.deserialize_result = deserialize_result

    def __call__(self, decision, execution_history, rate_limit=DescCounter()):
        """Instantiate Proxy."""
        task_exec_hist = SWFTaskExecutionHistory(execution_history, self.identity)
        task_decision = SWFActivityTaskDecision(decision, execution_history, self, rate_limit)
        return Proxy(task_exec_hist, task_decision, self.retry,
                     self.serialize_input, self.deserialize_result)


class SWFWorkflowProxyFactory(object):
    """Same as SWFActivityProxy but for sub-workflows."""

    def __init__(self, identity, name, version,
                 task_list=None,
                 workflow_duration=None,
                 decision_duration=None,
                 child_policy=None,
                 retry=(0, 0, 0),
                 serialize_input=None,
                 deserialize_result=None):
        self.identity = identity
        self.name = name
        self.version = version
        self.task_list = task_list
        self.workflow_duration = workflow_duration
        self.decision_duration = decision_duration
        self.child_policy = child_policy
        self.retry = retry
        self.serialize_input = serialize_input
        self.deserialize_result = deserialize_result

    def __call__(self, decision, execution_history, rate_limit):
        """Instantiate Proxy."""
        task_exec_hist = SWFTaskExecutionHistory(execution_history, self.identity)
        task_decision = SWFWorkflowTaskDecision(decision, execution_history, self, rate_limit)
        return Proxy(task_exec_hist, task_decision, self.retry,
                     self.serialize_input, self.deserialize_result)
