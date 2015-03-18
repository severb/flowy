try:
    from concurrent.futures import ProcessPoolExecutor
except ImportError:
    from futures import ProcessPoolExecutor

import json
import time
from collections import namedtuple
from functools import partial
from threading import Event
from threading import RLock

from flowy.backend.swf import SWFTaskExecutionHistory as TaskHistory
from flowy.backend.swf import JSONProxyEncoder
from flowy.base import _identity
from flowy.base import BoundProxy
from flowy.base import TracingBoundProxy
from flowy.base import TaskError
from flowy.base import Worker
from flowy.base import Workflow
from flowy.base import ExecutionTracer


class WorkflowRunner(object):
    def __init__(self, workflow, workflow_executor, activity_executor,
                 input_data, parent=None, w_id=None, tracer=None):
        self.workflow = workflow
        self.input_data = input_data
        self.workflow_executor = workflow_executor
        self.activity_executor = activity_executor
        self.parent = parent
        self.w_id = w_id
        self.running = set()
        self.results = dict()
        self.errors = dict()
        self.finish_order = []
        self.lock = RLock()
        self.stop = Event()
        self.will_restart = True
        self.history_updated = False
        self.tracer = tracer

    def child_runner(self, workflow, w_id, input_data):
        return WorkflowRunner(workflow, self.workflow_executor,
                              self.activity_executor, input_data,
                              parent=self, w_id=w_id)

    def set_decision(self, decision):
        self.decision = decision

    def run(self, wait=False):
        self.reschedule_decision()
        self.stop.wait()
        self.activity_executor.shutdown(wait=wait)
        self.workflow_executor.shutdown(wait=wait)
        if hasattr(self, 'result'):
            return self.result
        if hasattr(self, 'exception'):
            raise self.exception

    def reschedule_if_history_updated(self):
        with self.lock:
            if self.history_updated:
                self.history_updated = False
                self.reschedule_decision()
            else:
                self.will_restart = False

    def update_history_or_reschedule(self):
        with self.lock:
            if self.will_restart:
                self.history_updated = True
            else:
                self.history_updated = False
                self.will_restart = True
                self.reschedule_decision()

    def complete_activity_and_reschedule_decision(self, task_id, result):
        with self.lock:
            try:
                r = result.result()
            except Exception as e:
                self.set_error(task_id, str(e))
            else:
                self.set_result(task_id, json.dumps(r))
            self.update_history_or_reschedule()

    def fail_subwf_and_reschedule_decision(self, task_id, reason):
        with self.lock:
            self.set_error(task_id, str(reason))
            self.update_history_or_reschedule()

    def complete_subwf_and_reschedule_decision(self, task_id, result):
        with self.lock:
            self.set_result(task_id, result)
            self.update_history_or_reschedule()

    def reschedule_decision(self):
        with self.lock:
            try:
                tracer = None
                if self.tracer is not None:
                    tracer = self.tracer.copy()
                f = self.workflow_executor.submit(self.workflow,
                                                  self.ro_state(),
                                                  self.input_data,
                                                  tracer)
            except RuntimeError:
                return # The executor must be closed
            f.add_done_callback(partial(self.schedule_tasks))

    def schedule_tasks(self, result):
        try:
            result = result.result()
        except Exception as e:
            if self.parent is None:
                self.exception = TaskError(str(e))
                self.stop.set()
            else:
                self.parent.fail_subwf_and_reschedule_decision(self.w_id, e)
            return
        if result['type'] == 'finish':
            if self.parent is None:
                self.result = json.loads(result['result'])
                self.stop.set()
            else:
                self.parent.complete_subwf_and_reschedule_decision(
                    self.w_id, result['result'])
            return
        if result['type'] == 'fail':
            if self.parent is None:
                self.exception = TaskError(result['reason'])
                self.stop.set()
            else:
                self.parent.fail_subwf_and_reschedule_decision(
                    self.w_id, result['reason'])
            return
        assert result['type'] == 'schedule'
        with self.lock:
            for a in result.get('activities', []):
                self.set_running(a['id'])
                self.trace_activity(a)
            for w in result.get('workflows', []):
                self.set_running(w['id'])
                self.trace_workflow(w)
            self.trace_flush()
            for a in result.get('activities', []):
                try:
                    args, kwargs = json.loads(a['input_data'])
                    f = self.activity_executor.submit(a['f'], *args, **kwargs)
                    f.add_done_callback(partial(
                        self.complete_activity_and_reschedule_decision,
                        a['id']))
                except RuntimeError:
                    pass # The executor must be closed
            for w in result.get('workflows', []):
                try:
                    child_runner = self.child_runner(w['f'], w['id'],
                                                     w['input_data'])
                    child_runner.reschedule_decision()
                except RuntimeError:
                    pass # The executor must be closed
            self.reschedule_if_history_updated()

    def trace_activity(self, a):
        if self.tracer is None:
            return
        name, call_n, retry_n = a['id'].split('-')
        node_id = '%s-%s' % (name, call_n)
        assert not int(retry_n)
        self.tracer.schedule_activity(node_id, name)

    def trace_workflow(self, w):
        if self.tracer is None:
            return
        name, call_n, retry_n = w['id'].split('-')
        node_id = '%s-%s' % (name, call_n)
        assert not int(retry_n)
        self.tracer.schedule_workflow(node_id, name)

    def trace_flush(self):
        if self.tracer is None:
            return
        self.tracer.flush_scheduled()

    def trace_result(self, task_id, result):
        name, call_n, _ = task_id.split('-')
        node_id = '%s-%s' % (name, call_n)
        if self.tracer is None:
            return
        self.tracer.result(node_id, result)

    def trace_error(self, task_id, reason):
        name, call_n, _ = task_id.split('-')
        node_id = '%s-%s' % (name, call_n)
        if self.tracer is None:
            return
        self.tracer.error(node_id, reason)

    def set_running(self, call_key):
        with self.lock:
            self.running.add(call_key)

    def set_result(self, call_key, result):
        with self.lock:
            self.running.remove(call_key)
            self.results[call_key] = result
            self.finish_order.append(call_key)
            self.trace_result(call_key, result)

    def set_error(self, call_key, reason):
        with self.lock:
            self.running.remove(call_key)
            self.errors[call_key] = reason
            self.finish_order.append(call_key)
            self.trace_error(call_key, reason)

    def ro_state(self):
        with self.lock:
            return ROState(set(self.running), dict(self.results),
                           dict(self.errors), list(self.finish_order))

class ROState(object):
    def __init__(self, running=None, results=None, errors=None,
                 finish_order=None):
        self.running = running or set()
        self.results = results or {}
        self.errors = errors or {}
        self.finish_order = finish_order or []

    def is_running(self, call_key):
        return call_key in self.running

    def order(self, call_key):
        return self.finish_order.index(call_key)

    def has_result(self, call_key):
        return call_key in self.results

    def result(self, call_key):
        return self.results[call_key]

    def is_error(self, call_key):
        return call_key in self.errors

    def error(self, call_key):
        return self.errors[call_key]

    def is_timeout(self, call_key):
        return False

    def __repr__(self):
        if len(self.finish_order) > 6:
            order = (
                ' '.join(map(str, self.finish_order[:3]))
                + ' ... '
                + ' '.join(map(str, self.finish_order[-3:])))
        else:
            order = ' '.join(map(str, self.finish_order))
        return "<RUNNING: %d, RESULTS: %d, ERRORS: %d, ORDER: %s>" % (
            len(self.running), len(self.results), len(self.errors), order)


class Decision(dict):
    def __init__(self):
        self['type'] = 'schedule'
        self['activities'] = []
        self['workflows'] = []
        self.closed = False

    def fail(self, reason):
        if self.closed:
            return
        self.clear()
        self['type'] = 'fail'
        self['reason'] = reason
        self.closed = True

    def flush(self):
        self.closed = True

    def restart(self, input_data):
        if self.closed:
            return
        self.clear()
        self['type'] = 'restart'
        self['input_data'] = input_data
        self.closed = True

    def finish(self, result):
        if self.closed:
            return
        self.clear()
        self['type'] = 'finish'
        self['result'] = result
        self.closed = True

    def schedule_activity(self, call_key, input_data, f):
        if self.closed or 'activities' not in self:
            return
        self['activities'].append({
            'id': call_key,
            'input_data': input_data,
            'f': f})

    def schedule_workflow(self, call_key, input_data, f):
        if self.closed or 'workflows' not in self:
            return
        self['workflows'].append({
            'id': call_key,
            'input_data': input_data,
            'f': f})


class ActivityDecision(object):
    def __init__(self, decision, identity, f):
        self.decision = decision
        self.identity = identity
        self.f = f

    def fail(self, reason):
        self.decision.fail(reason)

    def schedule(self, call_number, retry_number, delay, input_data):
        self.decision.schedule_activity(
            '%s-%s-%s' % (self.identity, call_number, retry_number),
            input_data, self.f)


class WorkflowDecision(object):
    def __init__(self, decision, identity, f):
        self.decision = decision
        self.identity = identity
        self.f = f

    def fail(self, reason):
        self.decision.fail(reason)

    def schedule(self, call_number, retry_number, delay, input_data):
        self.decision.schedule_workflow(
            '%s-%s-%s' % (self.identity, call_number, retry_number),
            input_data, self.f)


Serializer = namedtuple('Serializer', 'serialize_input deserialize_result')

def serialize_input(*args, **kwargs):
    r = (args, kwargs)
    # Force the encoding only to walk the data structure and trigger any
    # errors or suspend tasks.
    # On py3 the proxy objects can't be pickled correctly, this fixes that
    # problem too.
    return json.dumps(r, cls=JSONProxyEncoder)

serializer = Serializer(serialize_input, json.loads)


class ActivityProxy(object):
    def __init__(self, identity, f):
        self.identity = identity
        self.f = f

    def __call__(self, decision, history, tracer):
        if tracer is None:
            return BoundProxy(
                serializer,
                TaskHistory(history, self.identity),
                ActivityDecision(decision, self.identity, self.f))
        return TracingBoundProxy(
            tracer,
            self.identity,
            serializer,
            TaskHistory(history, self.identity),
            ActivityDecision(decision, self.identity, self.f))


class WorkflowProxy(object):
    def __init__(self, identity, f):
        self.identity = identity
        self.f = f

    def __call__(self, decision, history, tracer):
        if tracer is None:
            return BoundProxy(
                serializer,
                TaskHistory(history, self.identity),
                WorkflowDecision(decision, self.identity, self.f))
        return TracingBoundProxy(
            self.identity,
            serializer,
            TaskHistory(history, self.identity),
            WorkflowDecision(decision, self.identity, self.f))


class LocalWorkflow(Workflow):

    def __init__(self, w, activity_workers=8, workflow_workers=2,
                 executor=ProcessPoolExecutor):
        super(LocalWorkflow, self).__init__()
        self.activity_workers = activity_workers
        self.workflow_workers = workflow_workers
        self.executor = executor
        self.worker = Worker()
        self.worker.register(self, w)

    def serialize_result(self, result):
        # See serialize_input for an explanation on why use json here.
        return json.dumps(result, cls=JSONProxyEncoder)

    def deserialize_input(self, input_data):
        return json.loads(input_data)

    def serialize_restart_input(self, *args, **kwargs):
        return serialize_input(*args, **kwargs)

    def conf_activity(self, dep_name, f):
        self.conf_proxy(dep_name, ActivityProxy(dep_name, f))

    def conf_workflow(self, dep_name, f):
        self.conf_proxy(dep_name, WorkflowProxy(dep_name, f))

    def __call__(self, state, input_data, tracer):
        d = Decision()
        self.worker(self, input_data, d, state, tracer)
        if not d['type'] == 'schedule' and tracer is not None:
            print tracer.as_dot()
        return d

    def run(self, *args, **kwargs):
        wait = kwargs.pop('_wait', False)
        a_executor = self.executor(max_workers=self.activity_workers)
        w_executor = self.executor(max_workers=self.workflow_workers)
        input_data = serializer.serialize_input(*args, **kwargs)
        tracer = ExecutionTracer()
        wr = WorkflowRunner(self, w_executor, a_executor, input_data,
                            tracer=tracer)
        r = wr.run(wait=wait)
        return r
