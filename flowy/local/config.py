from concurrent.futures import ProcessPoolExecutor

from flowy.config import WorkflowConfig
from flowy.local.decision import Decision
from flowy.local.proxy import ActivityProxy
from flowy.local.proxy import WorkflowProxy
from flowy.local.runner import RootWorkflowRunner
from flowy.proxy import Proxy
from flowy.tracer import ExecutionTracer
from flowy.worker import Worker


class LocalWorkflow(WorkflowConfig):
    def __init__(self, w,
                 activity_workers=8,
                 workflow_workers=2,
                 executor=ProcessPoolExecutor):
        super(LocalWorkflow, self).__init__()
        self.activity_workers = activity_workers
        self.workflow_workers = workflow_workers
        self.executor = executor
        self.worker = Worker()
        self.worker.register_task('local', self.wrap(w))

    def conf_activity(self, dep_name, f):
        self.conf_proxy_factory(dep_name, ActivityProxy(dep_name, f))

    def conf_workflow(self, dep_name, f):
        self.conf_proxy_factory(dep_name, WorkflowProxy(dep_name, f))

    def __call__(self, state, input_data, tracer):
        # NB: The final trace can be computed only on the last decision
        # thread/process
        d = Decision()
        self.worker('local', input_data, d,
                    d, state, tracer) # pass to proxies
        if d['type'] in ['finish', 'fail'] and tracer is not None:
            tracer.display()
        return d

    def run(self, *args, **kwargs):
        wait = kwargs.pop('_wait', False)
        tracer = None
        if kwargs.pop('_trace', False):
            tracer = ExecutionTracer()
        a_executor = self.executor(max_workers=self.activity_workers)
        w_executor = self.executor(max_workers=self.workflow_workers)
        input_data = Proxy.serialize_input(*args, **kwargs)
        wr = RootWorkflowRunner(self, w_executor, a_executor, input_data,
                                tracer=tracer)
        return wr.run(wait=wait)
