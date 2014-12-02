from contextlib import contextmanager

from flowy.proxy import _sentinel
from flowy.proxy import TaskProxy
from flowy.task import Task
from flowy.task import Workflow


class SWFActivity(Task):

    def __init__(self, swf_client, input, token):
        self._swf_client = swf_client
        self._token = token
        super(SWFActivity, self).__init__(input)

    def heartbeat(self):
        try:
            t = str(self._token)
            self._swf_client.record_activity_task_heartbeat(task_token=t)
        except SWFResponseError:
            logger.exception('Error while sending the heartbeat:')
            return False
        return True

    def _flush(self):
        pass

    def _fail(self, reason):
        try:
            self._swf_client.respond_activity_task_failed(
                reason=str(reason)[:256], task_token=str(self._token))
        except SWFResponseError:
            logger.exception('Error while failing the activity:')

    def _finish(self, result):
        try:
            result = self.serialize_result(result)
        except Exception as e:
            logger.exception('Error while serializing the result:')
            self._fail(e)
        try:
            self._swf_client.respond_activity_task_completed(
                result=str(result), task_token=str(self._token))
        except SWFResponseError:
            logger.exception('Error while finishing the activity:')


class SWFWorkflow(Workflow):
    def __init__(self, swf_client, token, timers, input, running, timedout,
                 results, errors, order):
        self._swf_client = swf_client
        self._token = token
        self._timers = timers
        super(SWFWorkflow, self).__init__(input, running, timedout, results,
                                          errors, order)

    def _restart(self, *args, **kwargs):
        decisions = Layer1Decisions()
        decision_duration, workflow_duration = self._timers_encode()
        # BOTO has a bug in this call when setting the decision_duration
        decisions.continue_as_new_workflow_execution(
            start_to_close_timeout=decision_duration,
            execution_start_to_close_timeout=workflow_duration,
            task_list=_str_or_none(self._task_list),
            input=str(input),
            tag_list=_tags_encode(tags))
        CANWEDA = 'continueAsNewWorkflowExecutionDecisionAttributes'
        last_decision_attrs = swf_decisions._data[-1][CANWEDA]
        STCT, TSTCT = 'startToCloseTimeout', 'taskStartToCloseTimeout'
        if STCT in last_decision_attrs:
            last_decision_attrs[TSTCT] = last_decision_attrs.pop(STCT)
        self._flush_layer1(decisions)

    def _complete(self, result):
        decisions = Layer1Decisions()
        result = str(self.serialize_result(result))
        decisions.complete_workflow_execution(result)
        self._flush_layer1(decisions)

    def _fail(self, reason):
        decisions = Layer1Decisions()
        decisions.fail_workflow_execution(reason=str(reason)[:256])
        self._flush_layer1(decisions)

    def _flush(self):
        decisions = Layer1Decisions()
        for proxy, call_key, a, kw, delay in self._scheduled:
            if call_key in self._timers:
                proxy.schedule(decisions, call_key, a, kw, delay)
            else:
                decisions.start_timer(start_to_fire_timeout=str(delay),
                                      timer_id='%s:timer' % call_key)
        self._flush_layer1(decisions)

    def _flush_layer1(self, decisions):
        try:
            task_completed = self._swf_client.respond_decision_task_completed(
                task_token=self._token, decisions=self._decisions._data)
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')


class JSONResult(Result):
    pass


class SWFActivityProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, retry=[0, 0, 0], error_handling=False):
        self._name = name
        self._version = version
        self._task_list = task_list
        self._hartbeat = heartbeat
        self._schedule_to_close = schedule_to_close
        self._schedule_to_start = schedule_to_start
        self._start_to_close = start_to_close
        super(SWFActivityProxy, self).__init__(retry, error_handling)

    @contextmanager
    def options(self, task_list=_sentinel, heartbeat=_sentinel,
                schedule_to_close=_sentinel, schedule_to_start=_sentinel,
                start_to_close=_sentinel, retry=_sentinel,
                error_handling=_sentinel):
        old_task_list = self._task_list
        old_heartbeat = self._heartbeat
        old_schedule_to_close = self._schedule_to_close
        old_schedule_to_start = self._schedule_to_start
        old_start_to_close = self._start_to_close
        if task_list is not _sentinel:
            self._task_list = task_list
        if heartbeat is not _sentinel:
            self._heartbeat = heartbeat
        if schedule_to_close is not _sentinel:
            self._schedule_to_close = schedule_to_close
        if schedule_to_start is not _sentinel:
            self._schedule_to_start = schedule_to_start
        if start_to_close is not _sentinel:
            self._start_to_close = start_to_close
        with super(SWFActivityProxy, self).options(retry, error_handling):
            yield
        self._task_list = old_task_list
        self._heartbeat = old_heartbeat
        self._schedule_to_close = old_schedule_to_close
        self._schedule_to_start = old_schedule_to_start
        self._start_to_close = old_start_to_close

    def schedule(self, scheduler, *args, **kwargs):
        input = str(self._serialize_arguments(a, kw))
        heartbeat, schedule_to_close, schedule_to_start, start_to_close = (
            self._timers_encode())
        swf_decisions.schedule_activity_task(
            str(call_key), str(self._name), str(self._version),
            heartbeat_timeout=heartbeat,
            schedule_to_close_timeout=schedule_to_close,
            schedule_to_start_timeout=schedule_to_start,
            start_to_close_timeout=start_to_close,
            task_list=_str_or_none(self._task_list),
            input=input)


class SWFWorkflowProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None, retry=[0, 0, 0], error_handling=False,
                 serialize_arguments=serialize_arguments,
                 deserialize_result=deserialize_result):
        self._spec = SWFWorkflowSpec(name, version, task_list,
                                     decision_duration, workflow_duration,
                                     serialize_arguments)
        super(SWFWorkflowProxy, self).__init__(retry, error_handling,
                                               deserialize_result)

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, retry=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            with super(SWFWorkflowProxy, self).options(retry, error_handling):
                yield

    def schedule(self, swf_decisions, call_key, a, kw):
        call_key = '%s-%s' % (uuid.uuid4(), call_key)
        return self._spec.schedule(swf_decisions, call_key, a, kw)


def swf_activity(version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, name=None):

    def wrapper(activity_factory):
        def callback(scanner, f_name, ob):
            if name is not None:
                f_name = name
            activity_spec = SWFActivitySpec(
                f_name, version, task_list, heartbeat, schedule_to_close,
                schedule_to_start, start_to_close)
            scanner.registry.add(activity_spec, activity_factory)
        venusian.attach(activity_factory, callback, category='activity')
        return activity_factory
    return wrapper


def swf_workflow(version, task_list=None, workflow_duration=None,
                 decision_duration=None, name=None):

    def wrapper(workflow_factory):

        scanner.attach(
            'swf_workflows',
            workflow_factory,
            version=version,
            task_list=task_list,
            workflow_duration=workflow_duration,
            decision_duration=decision_duration,
        )

        def callback(scanner, f_name, ob):
            if name is not None:
                f_name = name
            workflow_spec = SWFWorkflowSpec(
                f_name, version, task_list, decision_duration,
                workflow_duration)
            scanner.registry.add(workflow_spec, workflow_factory)
        venusian.attach(workflow_factory, callback, category='workflow')
        return workflow_factory
    return wrapper


class SWFTaskRegistry(TaskRegistry):
    def register_remote(self, swf_client):
        unregistered = []
        # sort it to be deterministic, helps on tests
        for spec in sorted(self._registry.keys()):
            if not spec.register_remote(swf_client):
                unregistered.append(spec)
        return unregistered


class SWFScanner(Scanner):
    def __init__(self, registry=None):
        if registry is None:
            registry = SWFTaskRegistry()
        super(SWFScanner, self).__init__(registry)

    def register_remote(self, swf_client):
        return self._registry.register_remote(swf_client)
