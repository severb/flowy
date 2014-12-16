import json
import logging

import venusian

from flowy.base import Workflow
from flowy.base import WorkflowConfig
from flowy.base import WorkflowRegistry

__all__ = ['SWFWorkflowConfig', 'SWFWorkflowRegistry', 'RegistrationError']


logger = logging.getLogger(__package__)
logging.basicConfig()


_CHILD_POLICY = ['TERMINATE', 'REQUEST_CANCEL', 'ABANDON', None]


_s_i = lambda *args, **kwargs: json.dumps((args, kwargs))
_d_i = json.loads
_s_r = json.dumps
_d_r = json.loads
_i = lambda x: x


class SWFWorkflowConfig(WorkflowConfig):
    """A configuration object suited for Amazon SWF Workflows.

    Use conf_activity and conf_workflow to configure workflow implementation
    dependencies.
    """

    category = 'swf_workflow'  # venusian category used for this type of confs

    @property
    def name(self):
        return None if self._name is None else str(self._name)

    @property
    def version(self):
        return str(self._version)

    def __init__(self, version, name=None, default_task_list=None,
                 default_workflow_duration=3600,
                 default_decision_duration=600,
                 default_child_policy='TERMINATE', rate_limit=64,
                 deserialize_input=_d_i, serialize_result=_s_r):
        """Initialize the config object.

        The timer values are in seconds, and the child policy should be either
        TERMINATE, REQUEST_CANCEL, ABANDON or None.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies.

        The rate_limit is used to limit the number of concurrent tasks. A value
        of None means no rate limit.

        The name is not required at this point but should be set before trying
        to register this config remotely and can be set later with
        set_alternate_name.
        """
        self._name = name
        self._version = version
        self.d_t_l = default_task_list
        self.d_w_d = default_workflow_duration
        self.d_d_d = default_decision_duration
        self.d_c_p = default_child_policy
        self.proxy_factory_registry = {}
        super(SWFWorkflowConfig, self).__init__(
            rate_limit, deserialize_input, serialize_result)

    def set_alternate_name(self, name):
        """Set the name of this workflow if one is not already set.

        Returns a configuration instance with the new name or the existing
        instance if the name was not changed.

        It's useful to return a new instance because if this config lacks a
        name it can be used to register multiple factories and fallback to each
        factory __name__ value.
        """
        if self.name is not None:
            return self
        klass = self.__class__
        # Make a clone since this config can be used as a decorator on multiple
        # workflow factories and each has a different name.
        c = klass(self.version, name=name,
            default_task_list=self.d_t_l,
            default_workflow_duration=self.d_w_d,
            default_decision_duration=self.d_d_d,
            default_child_policy=self.d_c_p,
            deserialize_input=self.deserialize_input,
            serialize_result=self.serialize_result)
        for dep_name, proxy_factory in self.proxy_factory_registry.iteritems():
            c.conf(dep_name, proxy_factory)
        return c

    def register(self, swf_layer1):
        """Register the workflow config in Amazon SWF if it's missing.

        If the workflow registration fails because there is already another
        workflow with this name and version registered, check if all the
        defaults have the same values.

        A name should be set before calling this method or RuntimeError is
        raised.

        If the registration is unsuccessful, the registered version is
        incompatible with this one or in case of SWF communication errors raise
        RegistrationError. ValueError is raised if any configuration values
        can't be converted to the required types.
        """
        registered_as_new = self.register_remote(swf_layer1)
        if not registered_as_new:
            success = self.check_compatible(swf_layer1)

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        name = self.name
        if name is None:
            raise RuntimeError('Name is not set.')
        d_t_l = _str_or_none(self.d_t_l),
        d_w_d = _timer_encode(self.d_w_d, 'default_workflow_duration')
        d_d_d = _timer_encode(self.d_d_d, 'default_decision_duration')
        d_c_p = _str_or_none(self.d_c_p)
        if child_policy not in _CHILD_POLICY:
            raise ValueError('Invalid child policy value: %r' % d_c_p)
        return name, self.version, d_t_l, d_w_d, d_d_d, d_c_p

    def register_remote(self, swf_layer1):
        """Register the workflow remotely.

        Returns True if registration is successful and False if another
        workflow with the same name is already registered.

        A name should be set before calling this method or RuntimeError is
        raised.

        Raise RegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.
        """
        name, version, d_t_l, d_w_d, d_d_d, d_c_p = self._cvt_values()
        try:
            swf_layer1.register_workflow_type(
                name=name, version=version, task_list=d_t_l,
                default_task_start_to_close_timeout=d_d_d,
                default_execution_start_to_close_timeout=d_w_d,
                default_child_policy=d_c_p)
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError as e:
            logger.exception('Error while registering the workflow:')
            raise RegistrationError(e)
        return True

    def check_compatible(self, swf_layer1):
        """Check if the remote config has the same defaults as this one.

        Returns True if the two configs are identical and False otherwise.

        A name should be set before calling this method or RuntimeError is
        raised.

        Raise RegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.
        """
        name, version, d_t_l, d_w_d, d_d_d, d_c_p = self._cvt_values()
        try:
            w = swf_layer1.describe_workflow_type(
                workflow_name=name, workflow_version=version)['configuration']
        except SWFResponseError as e:
            logger.exception('Error while checking workflow compatibility:')
            raise RegistrationError(e)
        return (
            w.get('defaultTaskList', {}).get('name') == d_t_l
            and w.get('defaultTaskStartToCloseTimeout') == d_d_d
            and w.get('defaultExecutionStartToCloseTimeout') == d_w_d
            and w.get('defaultChildPolicy') == d_c_p)

    def conf_activity(self, dep_name, version, name=None, task_list=None,
                      heartbeat=None, schedule_to_close=None,
                      schedule_to_start=None, start_to_close=None,
                      serialize_input=_s_i, deserialize_result=_d_i):
        """Configure an activity dependency for a workflow implementation.

        dep_name is the name of one of the workflow factory arguments
        (dependency). For example:

            class MyWorkflow:
                def __init__(self, a, b):  # Two dependencies: a and b
                    self.a = a
                    self.b = b
                def run(self, n):
                    pass

            cfg = SWFWorkflowConfig(version=1)
            cfg.conf_activity('a', name='MyActivity', version=1)
            cfg.conf_activity('b', version=2, task_list='my_tl')

        For convenience, if the activity name is missing, it will be the same
        as the dependency name.
        """
        if name is None:
            name = dep_name
        proxy = SWFActivityProxy(identity=dep_name, name=name, version=version,
                                 task_list=task_list, heartbeat=heartbeat,
                                 schedule_to_close=schedule_to_close,
                                 schedule_to_start=schedule_to_start,
                                 start_to_close=start_to_close,
                                 serialize_input=serialize_input,
                                 deserialize_result=deserialize_result)
        self.conf(dep_name, proxy)

    def conf_workflow(self, dep_name, version, name=None, task_list=None,
                      workflow_duration=None, decision_duration=None,
                      serialize_input=_s_i, deserialize_result=_d_i):
        """Same as conf_activity but for sub-workflows."""
        if name is None:
            name = dep_name
        proxy = SWFWorkflowProxy(identity=dep_name, name=name, version=version,
                                 task_list=task_list,
                                 workflow_duration=workflow_duration,
                                 decision_duration=decision_duration,
                                 serialize_input=serialize_input,
                                 deserialize_result=deserialize_result)
        self.conf(dep_name, proxy)


class SWFWorkflow(Workflow):
    """Bind a SWFWorkflowConfig instance and a workflow factory together.

    This will set the config alternate name to the workflow factory __name__.
    """
    def __init__(self, config, workflow_factory):
        config = config.set_alternate_name(workflow_factory.__name__)
        super(SWFWorkflow, self).__init__(config, workflow_factory)
        self.register = config.register  # delegate


class SWFWorkflowRegistry(WorkflowRegistry):
    """A factory for all registered workflows and their configs.

    The registered workflows are identified by their name and version.
    """

    categories = ['swf_workflow']
    WorkflowFactory = SWFWorkflow

    def _key(self, config):
        if config.name is None:
            raise ValueError('Cannot register an unnamed config object: %r' % config)
        return config.name, config.version

    def register_remote(self, layer1):
        """Register or check compatibility of all configs in Amazon SWF."""
        for workflow in self.registry.keys():
            workflow.register(layer1)

    def __call__(self, name, version, context, input):
        """Bind the corresponding config to the context and init a workflow.

        Raise value error if no config is found for this name and version,
        otherwise bind the config to the context and use it to instantiate the
        workflow.
        """
        key = str(name), str(version)
        return super(SWFWorkflowRegistry, self)(key, context, input)


class SWFActivityProxy(object):
    """An unbounded Amazon SWF activity proxy.

    This class is used by SWFWorkflowConfig for each activity configured.
    It must be bound to an execution context before it can be useful and uses
    double-dispatch trough ContextBoundProxy which has most of scheduling
    logic.
    """

    def __init__(self, identity, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, retry=[0, 0, 0], serialize_input=_s_i,
                 deserialize_result=_d_r):
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

    def bind(self, context, rate_limit=_DescCounter()):
        """Return a ContextBoundProxy instance that calls back schedule."""
        return ContextBoundProxy(self, context, rate_limit)

    def schedule(self, context, call_key, delay, *args, **kwargs):
        """Schedule the activity in the execution context.

        If any delay is set use SWF timers before really scheduling anything.
        """
        if int(delay) > 0 and not context.timer_ready(call_key):
            return context.schedule_timer(call_key, delay)
        try:
            input = self.serialize_input(*args, **kwargs)
        except Exception as e:
            logger.exception('Error while serializing activity input:')
            context.fail(e)
        else:
            context.schedule_activity(
                call_key, self.name, input, self.version, self.task_list,
                self.heartbeat, self.schedule_to_close, self.schedule_to_start,
                self.start_to_close)


class SWFWorkflowProxy(object):
    """Same as SWFActivityProxy but for sub-workflows."""
    def __init__(self, identity, name, version, task_list=None,
                 workflow_duration=None, decision_duration=None,
                 retry=[0, 0, 0], serialize_input=_s_i,
                 deserialize_result=_d_r):
        self.identity = identity
        self.name = name
        self.version = version
        self.task_list = task_list
        self.workflow_duration = workflow_duration
        self.decision_duration = decision_duration
        self.retry = retry
        self.serialize_input = serialize_input
        self.deserialize_result = deserialize_result

    def bind(self, context, rate_limit=_DescCounter()):
        return ContextBoundProxy(self, context, rate_limit)

    def schedule(self, context, call_key, delay, *args, **kwargs):
        if int(delay) > 0 and not context.timer_ready(call_key):
            return context.schedule_timer(call_key, delay)
        try:
            input = self.serialize_input(*args, **kwargs)
        except Exception as e:
            logger.exception('Error while serializing sub-workflow input:')
            context.fail(e)
        else:
            context.schedule_workflow(
                call_key, self.name, self.version, self.task_list,
                self.workflow_duration, self.decision_duration)


class SWFPoller(object):
    def __init__(self, layer1, task_list, domain):
        self.layer1 = layer1
        self.task_list = task_list
        self.domain = domain

    def poll_next(self):
        first_page = self._poll_first_page()
        token = first_page['taskToken']
        all_events = self._events(first_page)
        # Sometimes the first event in on the second page,
        # and the first page is empty
        first_event = next(all_events)
        assert first_event['eventType'] == 'WorkflowExecutionStarted'
        WESEA = 'workflowExecutionStartedEventAttributes'
        task_list = first_event[WESEA]['taskList']['name']
        decision_duration = first_event[WESEA]['taskStartToCloseTimeout']
        workflow_duration = first_event[WESEA]['executionStartToCloseTimeout']
        tags = first_event[WESEA].get('tagList', None)
        name = first_event[WESEA]['workflowType']['name']
        version = first_event[WESEA]['workflowType']['version']
        input = first_event[WESEA]['input']
        try:
            running, timedout, results, errors, order = (
                self._parse_events(events))
        except _PaginationError:
            # There's nothing better to do than to retry
            return self.poll_next()
        return name, version, input, SWFContext(layer1,
                task_list, decision_duration, workflow_duration, tags, #restart
                running, timedout, results, errors, order)

    def _poll_first_page(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self.layer1.poll_for_decision_task(
                    task_list=self.task_list, domain=domain
                )
            except SWFResponseError:
                logger.exception('Error while polling for decisions:')
        return swf_response

    def _poll_response_page(self, token):
        swf_response = None
        for _ in range(7):  # give up after a limited number of retries
            try:
                swf_response = self.layer1.poll_for_decision_task(
                    task_list=self.task_list,
                    next_page_token=page_token,
                    domain=self.domain)
                break
            except SWFResponseError:
                logger.exception('Error while polling for decision page:')
        else:
            raise _PaginationError()
        return swf_response

    def _events(self, first_page):
        page = first_page
        while 1:
            for event in page['events']:
                yield event
            if not page.get('nextPageToken'):
                break
            page = self._poll_response_page(token=page['nextPageToken'])

    def _parse_events(self, events):
        running, timedout = set(), set()
        results, errors = {}, {}
        order = []
        event2call = {}
        for e in events:
            e_type = e.get('eventType')
            if e_type == 'ActivityTaskScheduled':
                id = e['activityTaskScheduledEventAttributes']['activityId']
                event2call[e['eventId']] = id
                running.add(id)
            elif e_type == 'ActivityTaskCompleted':
                ATCEA = 'activityTaskCompletedEventAttributes'
                id = event2call[e[ATCEA]['scheduledEventId']]
                result = e[ATCEA]['result']
                running.remove(id)
                results[id] = result
                order.append(id)
            elif e_type == 'ActivityTaskFailed':
                ATFEA = 'activityTaskFailedEventAttributes'
                id = event2call[e[ATFEA]['scheduledEventId']]
                reason = e[ATFEA]['reason']
                running.remove(id)
                errors[id] = reason
                order.append(id)
            elif e_type == 'ActivityTaskTimedOut':
                ATTOEA = 'activityTaskTimedOutEventAttributes'
                id = event2call[e[ATTOEA]['scheduledEventId']]
                running.remove(id)
                timedout.add(id)
                order.append(id)
            elif e_type == 'ScheduleActivityTaskFailed':
                SATFEA = 'scheduleActivityTaskFailedEventAttributes'
                id = e[SATFEA]['activityId']
                reason = e[SATFEA]['cause']
                # when a job is not found it's not even started
                errors[id] = reason
                order.append(id)
            elif e_type == 'StartChildWorkflowExecutionInitiated':
                SCWEIEA = 'startChildWorkflowExecutionInitiatedEventAttributes'
                id = _subworkflow_id(e[SCWEIEA]['workflowId'])
                running.add(id)
            elif e_type == 'ChildWorkflowExecutionCompleted':
                CWECEA = 'childWorkflowExecutionCompletedEventAttributes'
                id = _subworkflow_id(
                    e[CWECEA]['workflowExecution']['workflowId'])
                result = e[CWECEA]['result']
                running.remove(id)
                results[id] = result
                order.append(id)
            elif e_type == 'ChildWorkflowExecutionFailed':
                CWEFEA = 'childWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(
                    e[CWEFEA]['workflowExecution']['workflowId'])
                reason = e[CWEFEA]['reason']
                running.remove(id)
                errors[id] = reason
                order.append(id)
            elif e_type == 'ChildWorkflowExecutionTimedOut':
                CWETOEA = 'childWorkflowExecutionTimedOutEventAttributes'
                id = _subworkflow_id(
                    e[CWETOEA]['workflowExecution']['workflowId'])
                running.remove(id)
                timedout.add(id)
                order.append(id)
            elif e_type == 'StartChildWorkflowExecutionFailed':
                SCWEFEA = 'startChildWorkflowExecutionFailedEventAttributes'
                id = _subworkflow_id(e[SCWEFEA]['workflowId'])
                reason = e[SCWEFEA]['cause']
                errors[id] = reason
                order.append(id)
            elif e_type == 'TimerStarted':
                id = e['timerStartedEventAttributes']['timerId']
                running.add(id)
                # while the timer is running, act as if the task itself is
                # running
                running.add(_timed_task_id(id))
            elif e_type == 'TimerFired':
                id = e['timerFiredEventAttributes']['timerId']
                running.remove(id)
                running.remove(_timed_task_id(id))
                results[id] = None
        return running, timedout, results, errors, order


class SWFContext(object):
    def __init__(self):
        pass

    def is_running(self, call_key):
        return call_key in self.running

    def is_result(self, call_key):
        return call_key in self.results

    def result(self, call_key):
        return self.results[call_key], self.order.index(call_key)

    def is_error(self, call_key):
        return call_key in self.errors

    def error(self, call_key):
        return self.errors[call_key], self.order.index(call_key)

    def is_timeout(self, call_key):
        return call_key in self.timedout

    def timeout(self, call_key):
        return self.order.index(call_key)

    def schedule_timer(self):
        pass

    def schedule_activity(self):
        pass

    def schedule_workflow(self):
        pass

    def fail(self, reason):
        pass

    def flush(self):
        pass

    def restart(self):
        pass


class _PaginationError(Exception):
    pass


class RegistrationError(Exception):
    pass


def _subworkflow_id(workflow_id):
    return workflow_id.rsplit('-', 1)[-1]


def _timed_task_id(timer_id):
    assert timer_id.endswith(':t')
    return timer_id[:-2]


def _timer_encode(val, name):
    if val is None:
        return None
    val = max(int(val), 0)
    if val == 0:
        raise ValueError('The value of %r must be a strictly positive integer: %r' % (name, val))
    return str(val)


def _str_or_none(val):
    if val is None:
        return None
    return str(val)
