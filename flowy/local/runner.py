import copy
from functools import partial
from threading import Event
from threading import RLock

from flowy import serialization
from flowy.result import TaskError


class WorkflowRunner(object):
    def __init__(self, workflow, workflow_executor, activity_executor,
                 input_data,
                 state=None,
                 tracer=None):
        self.workflow = workflow
        self.workflow_executor = workflow_executor
        self.activity_executor = activity_executor
        self.input_data = input_data
        self.state = state if state is not None else State()
        self.tracer = tracer
        self.lock = RLock()
        self.will_restart = True
        self.history_updated = False
        self.restarted = False

    def trace_activity(self, a):
        if self.tracer is None:
            return
        name, call_n, retry_n = a['id'].split('-')
        node_id = '%s-%s' % (name, call_n)
        assert int(retry_n) == 0
        self.tracer.schedule_activity(node_id, name)

    def trace_workflow(self, w):
        if self.tracer is None:
            return
        name, call_n, retry_n = w['id'].split('-')
        node_id = '%s-%s' % (name, call_n)
        assert int(retry_n) == 0
        self.tracer.schedule_workflow(node_id, name)

    def trace_flush(self):
        if self.tracer is None:
            return
        self.tracer.flush_scheduled()

    def trace_result(self, task_id, result):
        if self.tracer is None:
            return
        name, call_n, _ = task_id.split('-')
        node_id = '%s-%s' % (name, call_n)
        self.tracer.result(node_id, result)

    def trace_error(self, task_id, reason):
        if self.tracer is None:
            return
        name, call_n, _ = task_id.split('-')
        node_id = '%s-%s' % (name, call_n)
        self.tracer.error(node_id, reason)

    def reschedule_decision(self):
        if self.restarted:
            return
        # Any state that can mutate between the schedule time and the actual
        # execution time must be copied or otherwise it can be in an
        # inconsistent state. This includes the tracer if any and the state.
        tracer = self.tracer
        if tracer is not None:
            tracer = tracer.copy()
        try:
            f = self.workflow_executor.submit(self.workflow, self.state.copy(),
                                              self.input_data, tracer)
        except RuntimeError:
            return  # The executor must be closed
        f.add_done_callback(self.schedule_tasks)

    def schedule_tasks(self, result):
        with self.lock:
            if self.restarted:
                return
            try:
                result = result.result()
            except Exception as e:
                self.fail(e)
                return
            handle_func = 'handle_%s' % result['type']
            getattr(self, handle_func)(result)

    def fail(self, reason):
        raise NotImplementedError

    def handle_schedule(self, result):
        for a in result.get('activities', []):
            self.state.set_running(a['id'])
            self.trace_activity(a)
        for w in result.get('workflows', []):
            self.state.set_running(w['id'])
            self.trace_workflow(w)
        self.trace_flush()
        for a in result.get('activities', []):
            try:
                args, kwargs = serialization.loads(a['input_data'])
                f = self.activity_executor.submit(a['f'], *args, **kwargs)
                f.add_done_callback(partial(
                    self.complete_activity_and_reschedule_decision, a['id']))
            except RuntimeError:
                pass  # The executor must be closed
        for w in result.get('workflows', []):
            r = ChildWorkflowRunner(w['f'], self.workflow_executor,
                                    self.activity_executor, w['input_data'],
                                    parent=self,
                                    wid=w['id'])
            r.reschedule_decision()
        self.reschedule_if_history_updated()

    def handle_restart(self, _):
        self.restarted = True
        if self.tracer is not None:
            self.tracer.reset()

    def complete_activity_and_reschedule_decision(self, task_id, result):
        with self.lock:
            try:
                r = result.result()
            except Exception as e:
                self.state.set_error(task_id, str(e))
                self.trace_error(task_id, e)
            else:
                self.state.set_result(task_id, serialization.dumps(r))
                self.trace_result(task_id, r)
            self.update_history_or_reschedule()

    def fail_subwf_and_reschedule_decision(self, task_id, reason):
        with self.lock:
            self.state.set_error(task_id, str(reason))
            self.trace_error(task_id, reason)
            self.update_history_or_reschedule()

    def complete_subwf_and_reschedule_decision(self, task_id, result):
        with self.lock:
            self.state.set_result(task_id, result)
            self.trace_result(task_id, serialization.loads(result))
            self.update_history_or_reschedule()

    def update_history_or_reschedule(self):
        if self.will_restart:
            self.history_updated = True
        else:
            self.history_updated = False
            self.will_restart = True
            self.reschedule_decision()

    def reschedule_if_history_updated(self):
        if self.history_updated:
            self.history_updated = False
            self.reschedule_decision()
        else:
            self.will_restart = False


class RootWorkflowRunner(WorkflowRunner):
    def __init__(self, workflow, workflow_executor, activity_executor,
                 input_data,
                 state=None,
                 tracer=None):
        super(RootWorkflowRunner, self).__init__(workflow, workflow_executor,
                                                 activity_executor, input_data,
                                                 state=state,
                                                 tracer=tracer)
        self.stop = Event()

    def run(self, wait=False):
        self.reschedule_decision()
        self.stop.wait()
        self.activity_executor.shutdown(wait=wait)
        self.workflow_executor.shutdown(wait=wait)
        if hasattr(self, 'final_value'):
            if isinstance(self.final_value, Exception):
                raise self.final_value
            else:
                return self.final_value
        raise RuntimeError('No final value found.')

    def stop_running(self, final_value):
        self.final_value = final_value
        self.stop.set()

    def handle_fail(self, result):
        self.stop_running(TaskError(result['reason']))

    def handle_finish(self, result):
        self.stop_running(serialization.loads(result['result']))

    def fail(self, reason):
        self.stop_running(TaskError(str(reason)))

    def handle_restart(self, result):
        super(RootWorkflowRunner, self).handle_restart(result)
        RestartedRootRunner(self.workflow, self.workflow_executor,
                            self.activity_executor, result['input_data'], self,
                            tracer=self.tracer).reschedule_decision()


class RestartedRootRunner(WorkflowRunner):
    def __init__(self, workflow, workflow_executor, activity_executor,
                 input_data, root,
                 state=None,
                 tracer=None):
        super(RestartedRootRunner, self).__init__(
            workflow, workflow_executor, activity_executor, input_data,
            state=state,
            tracer=tracer)
        self.root = root

    def handle_fail(self, result):
        self.root.handle_fail(result)

    def handle_finish(self, result):
        self.root.handle_finish(result)

    def fail(self, reason):
        self.root.fail(reason)

    def handle_restart(self, result):
        super(RestartedRootRunner, self).handle_restart(result)
        r = RestartedRootRunner(self.workflow, self.workflow_executor,
                                self.activity_executor, result['input_data'],
                                self.root,
                                tracer=self.tracer)
        r.reschedule_decision()


class ChildWorkflowRunner(WorkflowRunner):
    def __init__(self, workflow, workflow_executor, activity_executor,
                 input_data, parent, wid,
                 state=None,
                 tracer=None):
        super(ChildWorkflowRunner, self).__init__(
            workflow, workflow_executor, activity_executor, input_data,
            state=state,
            tracer=tracer)
        self.parent = parent
        self.wid = wid

    def handle_fail(self, result):
        self.parent.fail_subwf_and_reschedule_decision(self.wid,
                                                       result['reason'])

    def handle_finish(self, result):
        self.parent.complete_subwf_and_reschedule_decision(self.wid,
                                                           result['result'])

    def fail(self, reason):
        self.parent.fail_subwf_and_reschedule_decision(self.wid, reason)

    def handle_restart(self, result):
        super(ChildWorkflowRunner, self).handle_restart(result)
        r = ChildWorkflowRunner(self.workflow, self.workflow_executor,
                                self.activity_executor, result['input_data'],
                                self.parent, self.wid,
                                tracer=self.tracer)
        r.reschedule_decision()


class State(object):
    def __init__(self):
        self.running = set()
        self.results = {}
        self.errors = {}
        self.finish_order = []

    def copy(self):
        s = State()
        s.__dict__ = copy.deepcopy(self.__dict__)
        return s

    def set_running(self, call_key):
        self.running.add(call_key)

    def set_result(self, call_key, result):
        self.running.remove(call_key)
        self.results[call_key] = result
        self.finish_order.append(call_key)

    def set_error(self, call_key, reason):
        self.running.remove(call_key)
        self.errors[call_key] = reason
        self.finish_order.append(call_key)

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
            order = (' '.join(map(str, self.finish_order[:3])) + ' ... ' +
                     ' '.join(map(str, self.finish_order[-3:])))
        else:
            order = ' '.join(map(str, self.finish_order))
        return "<RUNNING: %d, RESULTS: %d, ERRORS: %d, ORDER: %s>" % (
            len(self.running), len(self.results), len(self.errors), order)
