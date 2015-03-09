try:
    from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
except ImportError:
    from futures import ThreadPoolExecutor, ProcessPoolExecutor

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

import random
import threading
import time
from collections import namedtuple
from functools import partial
from threading import Event
from threading import RLock

from flowy.backend.swf import SWFTaskExecutionHistory as TaskHistory
from flowy.base import _identity
from flowy.base import BoundProxy
from flowy.base import TaskError
from flowy.base import Worker
from flowy.base import Workflow


class WorkflowRunner(object):
    def __init__(self, workflow, workflow_executor, activity_executor, args=[],
                 kwargs={}, parent=None, w_id=None):
        self.workflow = workflow
        self.args = args
        self.kwargs = kwargs
        self.workflow_executor = workflow_executor
        self.activity_executor = activity_executor
        self.parent = parent
        self.w_id = w_id
        self.running = set()
        self.results = dict()
        self.errors = dict()
        self.finish_order = []
        self.lock = RLock()
        self.decision = None
        self.queued = False
        self.stop = Event()

    def child_runner(self, workflow, w_id, args, kwargs):
        return WorkflowRunner(workflow, self.workflow_executor,
                              self.activity_executor, args, kwargs,
                              parent=self, w_id=w_id)

    def set_decision(self, decision):
        self.decision = decision

    def run(self):
        with self.lock:
            f = self.workflow_executor.submit(self.workflow, self.ro_state(),
                                              *self.args, **self.kwargs)
            self.set_decision(f)
            f.add_done_callback(self.schedule_tasks)
        self.stop.wait()
        self.activity_executor.shutdown()
        self.workflow_executor.shutdown()
        if hasattr(self, 'result'):
            return self.result
        if hasattr(self, 'exception'):
            raise self.exception

    def complete_activity_and_reschedule_decision(self, task_id, result):
        with self.lock:
            try:
                self.set_result(task_id, result.result())
            except Exception as e:
                self.set_error(task_id, str(e))
            self.reschedule_decision()

    def fail_subwf_and_reschedule_decision(self, task_id, reason):
        with self.lock:
            self.set_error(task_id, str(reason))
            self.reschedule_decision()

    def complete_subwf_and_reschedule_decision(self, task_id, result):
        with self.lock:
            self.set_result(task_id, result)
            self.reschedule_decision()

    def reschedule_decision(self):
        def submit_next_decision(result):
            with self.lock:
                self.queued = False
                f = self.workflow_executor.submit(self.workflow,
                                                  self.ro_state(),
                                                  *self.args, **self.kwargs)
                f.add_done_callback(self.schedule_tasks)
                self.decision = f
        with self.lock:
            if not self.queued:
                self.queued = True
                self.decision.add_done_callback(submit_next_decision)

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
                self.result = result['result']
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
        with self.lock:  # XXX: this may not be required
            for a in result.get('activities', []):
                self.set_running(a['id'])
            for w in result.get('workflows', []):
                self.set_running(w['id'])
        for a in result.get('activities', []):
            f = self.activity_executor.submit(a['f'], *a['args'],
                                              **a['kwargs'])
            f.add_done_callback(partial(
                self.complete_activity_and_reschedule_decision, a['id']))
        for w in result.get('workflows', []):
            child_runner = self.child_runner(w['f'], w['id'], w['args'],
                                             w['kwargs'])
            f = self.workflow_executor.submit(w['f'], child_runner.ro_state(),
                                              *w['args'], **w['kwargs'])
            child_runner.set_decision(f)
            f.add_done_callback(child_runner.schedule_tasks)

    def set_running(self, call_key):
        with self.lock:
            self.running.add(call_key)

    def set_result(self, call_key, result):
        with self.lock:
            self.running.remove(call_key)
            self.results[call_key] = result
            self.finish_order.append(call_key)

    def set_error(self, call_key, reason):
        with self.lock:
            self.running.remove(call_key)
            self.errors[call_key] = reason
            self.finish_order.append(call_key)

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
        self['args'], self['kwargs'] = input_data
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
            'args': input_data[0],
            'kwargs': input_data[1],
            'f': f})

    def schedule_workflow(self, call_key, input_data, f):
        if self.closed or 'workflows' not in self:
            return
        self['workflows'].append({
            'id': call_key,
            'args': input_data[0],
            'kwargs': input_data[1],
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
serializer = Serializer(lambda *args, **kwargs: (args, kwargs), _identity)


class ActivityProxy(object):
    def __init__(self, identity, f):
        self.identity = identity
        self.f = f

    def __call__(self, decision, history):
        return BoundProxy(
            serializer,
            TaskHistory(history, self.identity),
            ActivityDecision(decision, self.identity, self.f))


class WorkflowProxy(object):
    def __init__(self, identity, f):
        self.identity = identity
        self.f = f

    def __call__(self, decision, history):
        return BoundProxy(
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

    def conf_activity(self, dep_name, f):
        self.conf_proxy(dep_name, ActivityProxy(dep_name, f))

    def conf_workflow(self, dep_name, f):
        self.conf_proxy(dep_name, WorkflowProxy(dep_name, f))

    def __call__(self, state, *args, **kwargs):
        d = Decision()
        input_data = serializer.serialize_input(*args, **kwargs)
        self.worker(self, input_data, d, state)
        return d

    def run(self, *args, **kwargs):
        a_executor = self.executor(max_workers=self.activity_workers)
        w_executor = self.executor(max_workers=self.workflow_workers)
        wr = WorkflowRunner(self, w_executor, a_executor, args, kwargs)
        return wr.run()
