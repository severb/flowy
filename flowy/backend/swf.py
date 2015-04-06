import itertools
import json
import logging
import os
import platform
import socket
import sys
import uuid
from functools import partial

from boto.exception import SWFResponseError
from boto.swf.exceptions import SWFTypeAlreadyExistsError
from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions

from flowy.base import Activity
from flowy.base import BoundProxy
from flowy.base import is_result_proxy
from flowy.base import setup_default_logger
from flowy.base import Worker
from flowy.base import Workflow

__all__ = ['SWFWorkflow', 'SWFWorkflowWorker', 'SWFActivity',
           'SWFActivityWorker', 'SWFWorkflowStarter']

logger = logging.getLogger(__name__.split('.', 1)[0])

_CHILD_POLICY = ['TERMINATE', 'REQUEST_CANCEL', 'ABANDON', None]
_INPUT_SIZE = _RESULT_SIZE = 32768
_IDENTITY_SIZE = _REASON_SIZE = 256


class JSONProxyEncoder(json.JSONEncoder):
    # The pure Python implementation uses isinstance() which work on proxy
    # objects but the C implementation uses a stricter check that won't work on
    # proxy objects.
    def encode(self, o):
        if is_result_proxy(o):
            o = o.__wrapped__
        return super(JSONProxyEncoder, self).encode(o)

    def default(self, obj):
        if is_result_proxy(obj):
            return obj.__wrapped__
        return json.JSONEncoder.default(self, obj)

    # On py26 things are a bit worse...
    if sys.version_info[:2] == (2, 6):

        def _iterencode(self, o, markers=None):
            s = super(JSONProxyEncoder, self)
            if is_result_proxy(o):
                return s._iterencode(o.__wrapped__, markers)
            return s._iterencode(o, markers)

    # pypy uses simplejson, and ...
    if platform.python_implementation() == 'PyPy':

        def _JSONEncoder__encode(self, o, markers, builder,
                                 _current_indent_level):
            s = super(JSONProxyEncoder, self)
            if is_result_proxy(o):
                return s._JSONEncoder__encode(o.__wrapped__, markers, builder,
                                              _current_indent_level)
            return s._JSONEncoder__encode(o, markers, builder,
                                          _current_indent_level)


_serialize_input = lambda *args, **kwargs: json.dumps((args, kwargs),
                                                      cls=JSONProxyEncoder)
_serialize_result = partial(json.dumps, cls=JSONProxyEncoder)
_deserialize_input = _deserialize_result = json.loads


class SWFConfigMixin(object):
    def register_remote(self, swf_layer1, domain):
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
        registered_as_new = self.try_register_remote(swf_layer1, domain)
        if not registered_as_new:
            self.check_compatible(swf_layer1, domain)  # raises if incompatible

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
        clone_args = {'name': name}
        for prop, val in self.__dict__.items():
            if (prop == 'version' or prop.startswith('default') or
                prop.startswith('serialize') or
                prop.startswith('deserialize')):
                clone_args[prop] = val
        return klass(**clone_args)

    @property
    def key(self):
        """Use the name and the version to identify this config."""
        if self.name is None:
            raise RuntimeError('Name is not set.')
        return _proxy_key(self.name, self.version)


class SWFWorkflow(SWFConfigMixin, Workflow):
    """A configuration object suited for Amazon SWF Workflows.

    Use conf_activity and conf_workflow to configure workflow implementation
    dependencies.
    """

    category = 'swf_workflow'  # venusian category used for this type of confs

    def __init__(self, version,
                 name=None,
                 default_task_list=None,
                 default_workflow_duration=None,
                 default_decision_duration=None,
                 default_child_policy=None,
                 rate_limit=64,
                 deserialize_input=_deserialize_input,
                 serialize_result=_serialize_result,
                 serialize_restart_input=_serialize_input):
        """Initialize the config object.

        The timer values are in seconds, and the child policy should be either
        TERMINATE, REQUEST_CANCEL, ABANDON or None.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies pointing to this workflow.

        The rate_limit is used to limit the number of concurrent tasks. A value
        of None means no rate limit.

        The name is not required at this point but should be set before trying
        to register this config remotely and can be set later with
        set_alternate_name.
        """
        self.name = name
        self.version = version
        self.default_task_list = default_task_list
        self.default_workflow_duration = default_workflow_duration
        self.default_decision_duration = default_decision_duration
        self.default_child_policy = default_child_policy
        self.rate_limit = rate_limit
        self.proxy_factory_registry = {}
        super(SWFWorkflow, self).__init__(deserialize_input, serialize_result,
                                          serialize_restart_input)

    def init(self, workflow_factory, decision, execution_history):
        rate_limit = DescCounter(int(self.rate_limit))
        return super(SWFWorkflow, self).init(workflow_factory, decision,
                                             execution_history, rate_limit)

    def set_alternate_name(self, name):
        new_config = super(SWFWorkflow, self).set_alternate_name(name)
        if new_config is not self:
            for dep_name, proxy in self.proxy_factory_registry.items():
                new_config.conf_proxy(dep_name, proxy)
        return new_config

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        if self.name is None:
            raise RuntimeError('Name is not set.')
        d_t_l = _str_or_none(self.default_task_list)
        d_w_d = _timer_encode(self.default_workflow_duration,
                              'default_workflow_duration')
        d_d_d = _timer_encode(self.default_decision_duration,
                              'default_decision_duration')
        d_c_p = _cp_encode(self.default_child_policy)
        return str(self.name), str(self.version), d_t_l, d_w_d, d_d_d, d_c_p

    def try_register_remote(self, swf_layer1, domain):
        """Register the workflow remotely.

        Returns True if registration is successful and False if another
        workflow with the same name is already registered.

        A name should be set before calling this method or RuntimeError is
        raised.

        Raise _RegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.
        """
        n, version, d_t_l, d_w_d, d_d_d, d_c_p = self._cvt_values()
        try:
            swf_layer1.register_workflow_type(
                str(domain),
                name=n,
                version=version,
                task_list=d_t_l,
                default_execution_start_to_close_timeout=d_w_d,
                default_task_start_to_close_timeout=d_d_d,
                default_child_policy=d_c_p)
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError as e:
            if 'TypeAlreadyExistsFault' in str(e):  # eucalyptus
                return False
            logger.exception('Error while registering the workflow:')
            raise _RegistrationError(e)
        return True

    def check_compatible(self, swf_layer1, domain):
        """Check if the remote config has the same defaults as this one.

        A name should be set before calling this method or RuntimeError is
        raised.

        Raise _RegistrationError in case of SWF communication errors or
        incompatibility and ValueError if any configuration values can't be
        converted to the required types.
        """
        n, v, d_t_l, d_w_d, d_d_d, d_c_p = self._cvt_values()
        try:
            w_descr = swf_layer1.describe_workflow_type(str(domain), n, v)
            w_descr = w_descr['configuration']
        except SWFResponseError as e:
            logger.exception('Error while checking workflow compatibility:')
            raise _RegistrationError(e)
        r_d_t_l = w_descr.get('defaultTaskList', {}).get('name')
        if r_d_t_l != d_t_l:
            raise _RegistrationError(
                'Default task list for %r version %r does not match: %r != %r' %
                (n, v, r_d_t_l, d_t_l))
        r_d_d_d = w_descr.get('defaultTaskStartToCloseTimeout')
        if r_d_d_d != d_d_d:
            raise _RegistrationError(
                'Default decision duration for %r version %r does not match: %r != %r'
                % (n, v, r_d_d_d, d_d_d))
        r_d_w_d = w_descr.get('defaultExecutionStartToCloseTimeout')
        if r_d_w_d != d_w_d:
            raise _RegistrationError(
                'Default workflow duration for %r version %r does not match: %r != %r'
                % (n, v, r_d_w_d, d_w_d))
        r_d_c_p = w_descr.get('defaultChildPolicy')
        if r_d_c_p != d_c_p:
            raise _RegistrationError(
                'Default child policy for %r version %r does not match: %r != %r' %
                (n, v, r_d_c_p, d_c_p))

    def conf_activity(self, dep_name, version,
                      name=None,
                      task_list=None,
                      heartbeat=None,
                      schedule_to_close=None,
                      schedule_to_start=None,
                      start_to_close=None,
                      serialize_input=_serialize_input,
                      deserialize_result=_deserialize_input,
                      retry=(0, 0, 0)):
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
        proxy = SWFActivityProxy(identity=dep_name,
                                 name=name,
                                 version=version,
                                 task_list=task_list,
                                 heartbeat=heartbeat,
                                 schedule_to_close=schedule_to_close,
                                 schedule_to_start=schedule_to_start,
                                 start_to_close=start_to_close,
                                 serialize_input=serialize_input,
                                 deserialize_result=deserialize_result,
                                 retry=retry)
        self.conf_proxy(dep_name, proxy)

    def conf_workflow(self, dep_name, version,
                      name=None,
                      task_list=None,
                      workflow_duration=None,
                      decision_duration=None,
                      child_policy=None,
                      serialize_input=_serialize_input,
                      deserialize_result=_deserialize_input,
                      retry=(0, 0, 0)):
        """Same as conf_activity but for sub-workflows."""
        if name is None:
            name = dep_name
        proxy = SWFWorkflowProxy(identity=dep_name,
                                 name=name,
                                 version=version,
                                 task_list=task_list,
                                 workflow_duration=workflow_duration,
                                 decision_duration=decision_duration,
                                 child_policy=child_policy,
                                 serialize_input=serialize_input,
                                 deserialize_result=deserialize_result,
                                 retry=retry)
        self.conf_proxy(dep_name, proxy)


class SWFActivity(SWFConfigMixin, Activity):
    category = 'swf_activity'  # venusian category used for this type of confs

    def __init__(self, version,
                 name=None,
                 default_task_list=None,
                 default_heartbeat=None,
                 default_schedule_to_close=None,
                 default_schedule_to_start=None,
                 default_start_to_close=None,
                 deserialize_input=_deserialize_input,
                 serialize_result=_serialize_result):
        """Initialize the config object.

        The timer values are in seconds.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies pointing to this activity.

        The name is not required at this point but should be set before trying
        to register this config remotely and can be set later with
        set_alternate_name.
        """
        self.name = name
        self.version = version
        self.default_task_list = default_task_list
        self.default_heartbeat = default_heartbeat
        self.default_schedule_to_close = default_schedule_to_close
        self.default_schedule_to_start = default_schedule_to_start
        self.default_start_to_close = default_start_to_close
        super(SWFActivity, self).__init__(deserialize_input, serialize_result)

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        n = self.name
        if n is None:
            raise RuntimeError('Name is not set.')
        d_t_l = _str_or_none(self.default_task_list)
        d_h = _str_or_none(self.default_heartbeat)
        d_sch_c = _str_or_none(self.default_schedule_to_close)
        d_sch_s = _str_or_none(self.default_schedule_to_start)
        d_s_c = _str_or_none(self.default_start_to_close)
        return str(n), str(self.version), d_t_l, d_h, d_sch_c, d_sch_s, d_s_c

    def try_register_remote(self, swf_layer1, domain):
        """Same as SWFWorkflowConfig.try_register_remote."""
        n, version, d_t_l, d_h, d_sch_c, d_sch_s, d_s_c = self._cvt_values()
        try:
            swf_layer1.register_activity_type(
                str(domain),
                name=n,
                version=version,
                task_list=d_t_l,
                default_task_heartbeat_timeout=d_h,
                default_task_schedule_to_close_timeout=d_sch_c,
                default_task_schedule_to_start_timeout=d_sch_s,
                default_task_start_to_close_timeout=d_s_c)
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError as e:
            if 'TypeAlreadyExistsFault' in str(e):  # eucalyptus
                return False
            logger.exception('Error while registering the activity:')
            raise _RegistrationError(e)
        return True

    def check_compatible(self, swf_layer1, domain):
        """Same as SWFWorkflowConfig.check_compatible."""
        n, v, d_t_l, d_h, d_sch_c, d_sch_s, d_s_c = self._cvt_values()
        try:
            a_descr = swf_layer1.describe_activity_type(str(domain), n, v)
            a_descr = a_descr['configuration']
        except SWFResponseError as e:
            logger.exception('Error while checking activity compatibility:')
            raise _RegistrationError(e)
        r_d_t_l = a_descr.get('defaultTaskList', {}).get('name')
        if r_d_t_l != d_t_l:
            raise _RegistrationError(
                'Default task list for %r version %r does not match: %r != %r' %
                (n, v, r_d_t_l, d_t_l))
        r_d_h = a_descr.get('defaultTaskHeartbeatTimeout')
        if r_d_h != d_h:
            raise _RegistrationError(
                'Default heartbeat for %r version %r does not match: %r != %r' %
                (n, v, r_d_h, d_h))
        r_d_sch_c = a_descr.get('defaultTaskScheduleToCloseTimeout')
        if r_d_sch_c != d_sch_c:
            raise _RegistrationError(
                'Default schedule to close for %r version %r does not match: %r != %r'
                % (n, v, r_d_sch_c, d_sch_c))
        r_d_sch_s = a_descr.get('defaultTaskScheduleToStartTimeout')
        if r_d_sch_s != d_sch_s:
            raise _RegistrationError(
                'Default schedule to start for %r version %r does not match: %r != %r'
                % (n, v, r_d_sch_s, d_sch_s))
        r_d_s_c = a_descr.get('defaultTaskStartToCloseTimeout')
        if r_d_s_c != d_s_c:
            raise _RegistrationError(
                'Default start to close for %r version %r does not match: %r != %r'
                % (n, v, r_d_s_c, d_s_c))


class SWFWorker(Worker):
    def register_remote(self, layer1, domain):
        """Register or check compatibility of all configs in Amazon SWF."""
        for config, _ in self.registry.values():
            config.register_remote(layer1, domain)

    def register(self, config, impl):
        config = config.set_alternate_name(impl.__name__)
        super(SWFWorker, self).register(config, impl)


class SWFWorkflowWorker(SWFWorker):
    categories = ['swf_workflow']

    # Be explicit about what arguments are expected
    def __call__(self, key, input_data, decision, execution_history):
        # One extra argument for execution_history
        super(SWFWorkflowWorker, self).__call__(key, input_data, decision,
                                                execution_history)

    def break_loop(self):
        """Used to exit the loop in tests. Return True to break."""
        return False

    def run_forever(self, domain, task_list,
                    layer1=None,
                    setup_log=True,
                    register_remote=True,
                    identity=None):
        """Start an endless single threaded/single process worker loop.

        The worker polls endlessly for new decisions from the specified domain
        and task list and runs them.

        If reg_remote is set, all registered workflow are registered remotely.

        An identity can be set to track this worker in the SWF console,
        otherwise a default identity is generated from this machine domain and
        process pid.

        If setup_log is set, a default configuration for the logger is loaded.

        A custom SWF client can be passed in layer1, otherwise a default client
        is used.

        """
        if setup_log:
            setup_default_logger()
        identity = identity if identity is not None else _default_identity()
        identity = str(identity)[:_IDENTITY_SIZE]
        layer1 = layer1 if layer1 is not None else Layer1()
        if register_remote:
            self.register_remote(layer1, domain)
        try:
            while 1:
                if self.break_loop():
                    break
                key, input_data, exec_history, decision = poll_next_decision(
                    layer1, domain, task_list, identity)
                self(key, input_data, decision, exec_history)
        except KeyboardInterrupt:
            pass


class SWFActivityWorker(SWFWorker):
    categories = ['swf_activity']

    #Be explicit about what arguments are expected
    def __call__(self, key, input_data, decision):
        # No extra arguments are used
        super(SWFActivityWorker, self).__call__(key, input_data, decision)

    def break_loop(self):
        """Used to exit the loop in tests. Return True to break."""
        return False

    def run_forever(self, domain, task_list,
                    layer1=None,
                    setup_log=True,
                    register_remote=True,
                    identity=None):
        """Same as SWFWorkflowWorker.run_forever but for activities."""
        if setup_log:
            setup_default_logger()
        identity = identity if identity is not None else _default_identity()
        identity = str(identity)[:_IDENTITY_SIZE]
        layer1 = layer1 if layer1 is not None else Layer1()
        if register_remote:
            self.register_remote(layer1, domain)
        try:
            while 1:
                if self.break_loop():
                    break
                swf_response = {}
                while ('taskToken' not in swf_response or
                       not swf_response['taskToken']):
                    try:
                        swf_response = layer1.poll_for_activity_task(
                            domain=domain,
                            task_list=task_list,
                            identity=identity)
                    except SWFResponseError:
                        # add a delay before retrying?
                        logger.exception('Error while polling for activities:')

                at = swf_response['activityType']
                key = _proxy_key(at['name'], at['version'])
                input_data = swf_response['input']
                token = swf_response['taskToken']
                decision = SWFActivityDecision(layer1, token)
                self(key, input_data, decision)
        except KeyboardInterrupt:
            pass


class SWFActivityProxy(object):
    """An unbounded Amazon SWF activity proxy.

    This class is used by SWFWorkflowConfig for each activity configured.
    It must be bound to an execution context before it can be useful and uses
    double-dispatch trough ContextBoundProxy which has most of scheduling
    logic.
    """

    def __init__(self, identity, name, version,
                 task_list=None,
                 heartbeat=None,
                 schedule_to_close=None,
                 schedule_to_start=None,
                 start_to_close=None,
                 retry=(0, 0, 0),
                 serialize_input=_serialize_input,
                 deserialize_result=_deserialize_result):
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

    def __call__(self, decision, execution_history, rate_limit):
        """Return a BoundProxy instance."""
        task_exec_hist = SWFTaskExecutionHistory(execution_history,
                                                 self.identity)
        task_decision = SWFActivityTaskDecision(decision, execution_history,
                                                self, rate_limit)
        return BoundProxy(self, task_exec_hist, task_decision, self.retry)


class SWFWorkflowProxy(object):
    """Same as SWFActivityProxy but for sub-workflows."""

    def __init__(self, identity, name, version,
                 task_list=None,
                 workflow_duration=None,
                 decision_duration=None,
                 child_policy=None,
                 retry=(0, 0, 0),
                 serialize_input=_serialize_input,
                 deserialize_result=_deserialize_result):
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
        """Return a BoundProxy instance."""
        task_exec_hist = SWFTaskExecutionHistory(execution_history,
                                                 self.identity)
        task_decision = SWFWorkflowTaskDecision(decision, execution_history,
                                                self, rate_limit)
        return BoundProxy(self, task_exec_hist, task_decision, self.retry)


class SWFExecutionHistory(object):
    def __init__(self, running, timedout, results, errors, order):
        self.running = running
        self.timedout = timedout
        self.results = results
        self.errors = errors
        self.order_ = order

    def is_running(self, call_key):
        return str(call_key) in self.running

    def order(self, call_key):
        return self.order_.index(str(call_key))

    def has_result(self, call_key):
        return str(call_key) in self.results

    def result(self, call_key):
        return self.results[str(call_key)]

    def is_error(self, call_key):
        return str(call_key) in self.errors

    def error(self, call_key):
        return self.errors[str(call_key)]

    def is_timeout(self, call_key):
        return str(call_key) in self.timedout

    def is_timer_ready(self, call_key):
        return _timer_key(call_key) in self.results

    def is_timer_running(self, call_key):
        return _timer_key(call_key) in self.running


class SWFTaskExecutionHistory(object):
    def __init__(self, exec_history, identity):
        self.exec_history = exec_history
        self.identity = identity

    def k(self, call_number, retry_number):
        return _task_key(self.identity, call_number, retry_number)

    def __getattr__(self, fname):
        """Compute the key and delegate to exec_history."""
        if fname not in ['is_running', 'is_timeout', 'is_error', 'has_result',
                         'result', 'order', 'error']:
            return getattr(super(SWFTaskExecutionHistory, self), fname)

        delegate_to = getattr(self.exec_history, fname)

        def clos(call_number, retry_number):
            return delegate_to(self.k(call_number, retry_number))

        setattr(self, fname, clos)  # cache it
        return clos


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
        decisions.fail_workflow_execution(reason=str(reason)[:_REASON_SIZE])
        self.flush()

    def flush(self):
        """Flush the decisions; no other decisions can be sent after that."""
        if self.closed:
            return
        self.closed = True
        try:
            self.layer1.respond_decision_task_completed(
                task_token=str(self.token),
                decisions=self.decisions._data)
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')
            # ignore the error and let the decision timeout and retry

    def restart(self, input_data):
        """Restart the workflow and flush.

        Any other decisions queued are cleared.
        """
        decisions = self.decisions = Layer1Decisions()
        decisions.continue_as_new_workflow_execution(
            start_to_close_timeout=_str_or_none(self.decision_duration),
            execution_start_to_close_timeout=_str_or_none(self.
                                                          workflow_duration),
            task_list=_str_or_none(self.task_list),
            input=str(input_data)[:_INPUT_SIZE],
            tag_list=_tags(self.tags),
            child_policy=_cp_encode(self.child_policy))
        self.flush()

    def finish(self, result):
        """Finish the workflow execution and flush.

        Any other decisions queued are cleared.
        """
        decisions = self.decisions = Layer1Decisions()
        result = str(result)
        if len(result) > _RESULT_SIZE:
            self.fail("Result too large: %s/%s" % (len(result), _RESULT_SIZE))
        else:
            decisions.complete_workflow_execution(result)
            self.flush()

    def schedule_timer(self, call_key, delay):
        """Schedule a timer. This is used to delay execution of tasks."""
        self.decisions.start_timer(timer_id=_timer_key(str(call_key)),
                                   start_to_fire_timeout=str(delay))

    def schedule_activity(self, call_key, name, version, input_data, task_list,
                          heartbeat, schedule_to_close, schedule_to_start,
                          start_to_close):
        """Schedule an activity execution."""
        self.decisions.schedule_activity_task(
            str(call_key), str(name), str(version),
            heartbeat_timeout=_str_or_none(heartbeat),
            schedule_to_close_timeout=_str_or_none(schedule_to_close),
            schedule_to_start_timeout=_str_or_none(schedule_to_start),
            start_to_close_timeout=_str_or_none(start_to_close),
            task_list=_str_or_none(task_list),
            input=str(input_data))

    def schedule_workflow(self, call_key, name, version, input_data, task_list,
                          workflow_duration, decision_duration, child_policy):
        """Schedule a workflow execution."""
        call_key = _subworkflow_key(call_key)
        self.decisions.start_child_workflow_execution(
            str(name), str(version), str(call_key),
            task_start_to_close_timeout=_str_or_none(decision_duration),
            execution_start_to_close_timeout=_str_or_none(workflow_duration),
            task_list=_str_or_none(task_list),
            input=str(input_data),
            child_policy=_cp_encode(child_policy))


class SWFWorkflowTaskDecision(object):
    def __init__(self, decision, execution_history, proxy, rate_limit):
        self.decision = decision
        self.execution_history = execution_history
        self.proxy = proxy
        self.rate_limit = rate_limit

    def fail(self, reason):
        self.decision.fail(reason)

    def schedule(self, call_number, retry_number, delay, input_data):
        if not self.rate_limit.consume():
            return
        task_key = _task_key(self.proxy.identity, call_number, retry_number)
        if delay:
            if self.execution_history.is_timer_ready(task_key):
                self._schedule(task_key, input_data)
            elif not self.execution_history.is_timer_running(task_key):
                self.decision.schedule_timer(task_key, delay)
        else:
            self._schedule(task_key, input_data)

    def _schedule(self, task_key, input_data):
        self.decision.schedule_workflow(
            task_key, self.proxy.name, self.proxy.version, input_data,
            self.proxy.task_list, self.proxy.workflow_duration,
            self.proxy.decision_duration, self.proxy.child_policy)


class SWFActivityTaskDecision(SWFWorkflowTaskDecision):
    def _schedule(self, task_key, input_data):
        self.decision.schedule_activity(
            task_key, self.proxy.name, self.proxy.version, input_data,
            self.proxy.task_list, self.proxy.heartbeat,
            self.proxy.schedule_to_close, self.proxy.schedule_to_start,
            self.proxy.start_to_close)


class SWFActivityDecision(object):
    def __init__(self, layer1, token):
        self.layer1 = layer1
        self.token = token

    def heartbeat(self):
        try:
            self.layer1.record_activity_task_heartbeat(
                task_token=str(self.token))
        except SWFResponseError:
            logger.exception('Error while sending the heartbeat:')
            return False
        return True

    def fail(self, reason):
        try:
            self.layer1.respond_activity_task_failed(
                reason=str(reason)[:256],
                task_token=str(self.token))
        except SWFResponseError:
            logger.exception('Error while failing the activity:')
            return False
        return True

    def flush(self):
        pass

    def restart(self, input_data):
        self.fail("Can't restart activities.")

    def finish(self, result):
        result = str(result)
        if len(result) > _RESULT_SIZE:
            self.fail("Result too large: %s/%s" % (len(result), _RESULT_SIZE))
        try:
            self.layer1.respond_activity_task_completed(
                result=result,
                task_token=str(self.token))
        except SWFResponseError:
            logger.exception('Error while finishing the activity:')
            return False
        return True


def SWFWorkflowStarter(domain, name, version,
                       layer1=None,
                       task_list=None,
                       decision_duration=None,
                       workflow_duration=None,
                       wid=None,
                       tags=None,
                       serialize_input=_serialize_input,
                       child_policy=None):
    """Prepare to start a new workflow, returns a callable.

    The callable should be called only with the input arguments and will
    start the workflow.
    """

    def really_start(*args, **kwargs):
        """Use this function to start a workflow by passing in the args."""
        l1 = layer1 if layer1 is not None else Layer1()
        l_wid = wid  # closue hack
        if l_wid is None:
            l_wid = uuid.uuid4()
        try:
            r = l1.start_workflow_execution(
                str(domain), str(l_wid), str(name), str(version),
                task_list=_str_or_none(task_list),
                execution_start_to_close_timeout=_str_or_none(workflow_duration),
                task_start_to_close_timeout=_str_or_none(decision_duration),
                input=str(serialize_input(*args, **kwargs))[:_INPUT_SIZE],
                child_policy=_cp_encode(child_policy),
                tag_list=_tags(tags))
        except SWFResponseError:
            logger.exception('Error while starting the workflow:')
            return None
        return r['runId']

    return really_start


def poll_next_decision(layer1, domain, task_list, identity=None):
    """Poll a decision and create a SWFWorkflowContext instance."""
    first_page = poll_first_page(layer1, domain, task_list, identity)
    token = first_page['taskToken']
    all_events = events(layer1, domain, task_list, first_page, identity)
    # Sometimes the first event in on the second page,
    # and the first page is empty
    first_event = next(all_events)
    assert first_event['eventType'] == 'WorkflowExecutionStarted'
    wesea = 'workflowExecutionStartedEventAttributes'
    assert first_event[wesea]['taskList']['name'] == task_list
    decision_duration = first_event[wesea]['taskStartToCloseTimeout']
    workflow_duration = first_event[wesea]['executionStartToCloseTimeout']
    tags = first_event[wesea].get('tagList', None)
    child_policy = first_event[wesea]['childPolicy']
    name = first_event[wesea]['workflowType']['name']
    version = first_event[wesea]['workflowType']['version']
    input_data = first_event[wesea]['input']
    try:
        running, timedout, results, errors, order = load_events(all_events)
    except _PaginationError:
        # There's nothing better to do than to retry
        return poll_next_decision(layer1, task_list, domain, identity)
    execution_history = SWFExecutionHistory(running, timedout, results, errors,
                                            order)
    decision = SWFWorkflowDecision(layer1, token, name, version, task_list,
                                   decision_duration, workflow_duration, tags,
                                   child_policy)
    return _proxy_key(name, version), input_data, execution_history, decision


def poll_first_page(layer1, domain, task_list, identity=None):
    """Return the response from loading the first page.

    In case of errors, empty responses or whatnot retry until a valid response.
    """
    swf_response = {}
    while 'taskToken' not in swf_response or not swf_response['taskToken']:
        try:
            swf_response = layer1.poll_for_decision_task(
                str(domain), str(task_list), _str_or_none(identity))
        except SWFResponseError:
            logger.exception('Error while polling for decisions:')
    return swf_response


def poll_response_page(layer1, domain, task_list, token, identity=None):
    """Return a specific page. In case of errors retry a number of times."""
    swf_response = None
    for _ in range(7):  # give up after a limited number of retries
        try:
            swf_response = layer1.poll_for_decision_task(
                str(domain), str(task_list), _str_or_none(identity),
                next_page_token=str(token))
            break
        except SWFResponseError:
            logger.exception('Error while polling for decision page:')
    else:
        raise _PaginationError()
    return swf_response


def events(layer1, domain, task_list, first_page, identity=None):
    """Load pages one by one and generate all events found."""
    page = first_page
    while 1:
        for event in page['events']:
            yield event
        if not page.get('nextPageToken'):
            break
        page = poll_response_page(layer1, domain, task_list,
                                  page['nextPageToken'], identity)


def load_events(event_iter):
    """Combine all events in their order.

    This returns a tuple of the following things:
        running  - a set of the ids of running tasks
        timedout - a set of the ids of tasks that have timedout
        results  - a dictionary of id -> result for each finished task
        errors   - a dictionary of id -> error message for each failed task
        order    - an list of task ids in the order they finished
    """
    running, timedout = set(), set()
    results, errors = {}, {}
    order = []
    event2call = {}
    for event in event_iter:
        e_type = event.get('eventType')
        if e_type == 'ActivityTaskScheduled':
            eid = event['activityTaskScheduledEventAttributes']['activityId']
            event2call[event['eventId']] = eid
            running.add(eid)
        elif e_type == 'ActivityTaskCompleted':
            atcea = 'activityTaskCompletedEventAttributes'
            eid = event2call[event[atcea]['scheduledEventId']]
            result = event[atcea]['result']
            running.remove(eid)
            results[eid] = result
            order.append(eid)
        elif e_type == 'ActivityTaskFailed':
            atfea = 'activityTaskFailedEventAttributes'
            eid = event2call[event[atfea]['scheduledEventId']]
            reason = event[atfea]['reason']
            running.remove(eid)
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'ActivityTaskTimedOut':
            attoea = 'activityTaskTimedOutEventAttributes'
            eid = event2call[event[attoea]['scheduledEventId']]
            running.remove(eid)
            timedout.add(eid)
            order.append(eid)
        elif e_type == 'ScheduleActivityTaskFailed':
            satfea = 'scheduleActivityTaskFailedEventAttributes'
            eid = event[satfea]['activityId']
            reason = event[satfea]['cause']
            # when a job is not found it's not even started
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'StartChildWorkflowExecutionInitiated':
            scweiea = 'startChildWorkflowExecutionInitiatedEventAttributes'
            eid = _subworkflow_call_key(event[scweiea]['workflowId'])
            running.add(eid)
        elif e_type == 'ChildWorkflowExecutionCompleted':
            cwecea = 'childWorkflowExecutionCompletedEventAttributes'
            eid = _subworkflow_call_key(
                event[cwecea]['workflowExecution']['workflowId'])
            result = event[cwecea]['result']
            running.remove(eid)
            results[eid] = result
            order.append(eid)
        elif e_type == 'ChildWorkflowExecutionFailed':
            cwefea = 'childWorkflowExecutionFailedEventAttributes'
            eid = _subworkflow_call_key(
                event[cwefea]['workflowExecution']['workflowId'])
            reason = event[cwefea]['reason']
            running.remove(eid)
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'ChildWorkflowExecutionTimedOut':
            cwetoea = 'childWorkflowExecutionTimedOutEventAttributes'
            eid = _subworkflow_call_key(
                event[cwetoea]['workflowExecution']['workflowId'])
            running.remove(eid)
            timedout.add(eid)
            order.append(eid)
        elif e_type == 'StartChildWorkflowExecutionFailed':
            scwefea = 'startChildWorkflowExecutionFailedEventAttributes'
            eid = _subworkflow_call_key(event[scwefea]['workflowId'])
            reason = event[scwefea]['cause']
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'TimerStarted':
            eid = event['timerStartedEventAttributes']['timerId']
            running.add(eid)
        elif e_type == 'TimerFired':
            eid = event['timerFiredEventAttributes']['timerId']
            running.remove(eid)
            results[eid] = None
    return running, timedout, results, errors, order


class DescCounter(object):
    """A simple semaphore-like descendent counter."""

    def __init__(self, to=None):
        if to is None:
            self.iterator = itertools.repeat(True)
        else:
            self.iterator = itertools.chain(itertools.repeat(True, to),
                                            itertools.repeat(False))

    def consume(self):
        """Conusme one position; returns True if positions are available."""
        return next(self.iterator)


def _default_identity():
    """Generate a local identity for this process."""
    identity = "%s-%s" % (socket.getfqdn(), os.getpid())
    return identity[-_IDENTITY_SIZE:]  # keep the most important part


class _PaginationError(Exception):
    """Can't retrieve the next page after X retries."""


class _RegistrationError(Exception):
    """Can't register a task remotely because of default config conflicts."""


def _subworkflow_id(workflow_id):
    return workflow_id.rsplit('-', 1)[-1]


def _timer_key(call_key):
    return '%s:t' % call_key


def _subworkflow_key(call_key):
    return '%s:%s' % (uuid.uuid4(), call_key)


def _subworkflow_call_key(subworkflow_key):
    return subworkflow_key.split(':')[-1]


def _timer_encode(val, name):
    if val is None:
        return None
    val = max(int(val), 0)
    if val == 0:
        raise ValueError(
            'The value of %r must be a strictly positive integer: %r' %
            (name, val))
    return str(val)


def _str_or_none(val):
    if val is None:
        return None
    return str(val)


def _cp_encode(val):
    if val is not None:
        val = str(val).upper()
    if val not in _CHILD_POLICY:
        raise ValueError('Invalid child policy value: %r' % val)
    return val


def _tags(tags):
    if tags is None:
        return None
    return list(set(str(t) for t in tags))[:5]


def _task_key(identity, call_number, retry_number):
    return '%s-%s-%s' % (identity, call_number, retry_number)


def _proxy_key(name, version):
    return str(name), str(version)
