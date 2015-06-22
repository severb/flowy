from flowy.local.decision import ActivityDecision
from flowy.local.decision import WorkflowDecision
from flowy.proxy import Proxy
from flowy.swf.history import SWFTaskExecutionHistory as TaskHistory
from flowy.tracer import TracingProxy


class ActivityProxy(object):
    def __init__(self, identity, f):
        self.identity = identity
        self.f = f

    def __call__(self, decision, history, tracer):
        th = TaskHistory(history, self.identity)
        ad = ActivityDecision(decision, self.identity, self.f)
        if tracer is None:
            return Proxy(th, ad)
        return TracingProxy(tracer, self.identity, th, ad)


class WorkflowProxy(object):
    def __init__(self, identity, f):
        self.identity = identity
        self.f = f

    def __call__(self, decision, history, tracer):
        th = TaskHistory(history, self.identity)
        wd = WorkflowDecision(decision, self.identity, self.f)
        if tracer is None:
            return Proxy(th, wd)
        return TracingProxy(tracer, self.identity, th, wd)
