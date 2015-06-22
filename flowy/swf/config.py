import functools

from boto.exception import SWFResponseError
from boto.swf.exceptions import SWFTypeAlreadyExistsError

from flowy.swf.proxy import SWFActivityProxyFactory
from flowy.swf.proxy import SWFWorkflowProxyFactory
from flowy.config import ActivityConfig
from flowy.config import WorkflowConfig
from flowy.utils import DescCounter
from flowy.utils import logger
from flowy.utils import str_or_none


class SWFConfigMixin(object):
    def register_remote(self, swf_layer1, domain, name, version):
        """Register the config in Amazon SWF if it's missing.

        If the task registration fails because there is another task registered
        with the same name and version, check if all the defaults have the same
        values.

        If the registration is unsuccessful and the registered version is
        incompatible with this one or in case of SWF communication errors raise
        SWFRegistrationError. ValueError is raised if any configuration values
        can't be converted to the required types.
        """
        registered_as_new = self.try_register_remote(swf_layer1, domain, name, version)
        if not registered_as_new:
            self.check_compatible(swf_layer1, domain, name, version)  # raises if incompatible

    def register(self, registry, key, func):
        name, version = key
        if name is None:
            name = func.__name__
        name, version = str(name), str(version)
        registry.register_task((name, version), self.wrap(func))
        registry.add_remote_reg_callback(
            functools.partial(self.register_remote, name=name, version=version))

    def __call__(self, version, name=None):
        key = (name, version)
        return super(SWFConfigMixin, self).__call__(key)


class SWFActivityConfig(SWFConfigMixin, ActivityConfig):
    """A configuration object for Amazon SWF Activities."""
    category = 'swf_activity'  # venusian category used for this type of confs

    def __init__(self,
                 default_task_list=None,
                 default_heartbeat=None,
                 default_schedule_to_close=None,
                 default_schedule_to_start=None,
                 default_start_to_close=None,
                 deserialize_input=None,
                 serialize_result=None):
        """Initialize the config object.

        The timer values are in seconds.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies pointing to this activity.

        The name is optional. If no name is set, it will default to the
        function name.
        """
        super(SWFActivityConfig, self).__init__(deserialize_input, serialize_result)
        self.default_task_list = default_task_list
        self.default_heartbeat = default_heartbeat
        self.default_schedule_to_close = default_schedule_to_close
        self.default_schedule_to_start = default_schedule_to_start
        self.default_start_to_close = default_start_to_close

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        d_t_l = str_or_none(self.default_task_list)
        d_h = timer_encode(self.default_heartbeat, 'default_heartbeat')
        d_sch_c = timer_encode(self.default_schedule_to_close, 'default_schedule_to_close')
        d_sch_s = timer_encode(self.default_schedule_to_start, 'default_schedule_to_start')
        d_s_c = timer_encode(self.default_start_to_close, 'default_start_to_close')
        return d_t_l, d_h, d_sch_c, d_sch_s, d_s_c

    def try_register_remote(self, swf_layer1, domain, name, version):
        """Register the activity remotely.

        Returns True if registration is successful and False if another
        activity with the same name is already registered.

        Raise SWFRegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.
        """
        d_t_l, d_h, d_sch_c, d_sch_s, d_s_c = self._cvt_values()
        try:
            swf_layer1.register_activity_type(
                str(domain),
                name=str(name),
                version=str(version),
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
            raise SWFRegistrationError(e)
        return True

    def check_compatible(self, swf_layer1, domain, name, version):
        """Check if the remote config has the same defaults as this one.

        Raise SWFRegistrationError in case of SWF communication errors or
        incompatibility and ValueError if any configuration values can't be
        converted to the required types.
        """
        d_t_l, d_h, d_sch_c, d_sch_s, d_s_c = self._cvt_values()
        try:
            a_descr = swf_layer1.describe_activity_type(str(domain), str(name), str(version))
            a_descr = a_descr['configuration']
        except SWFResponseError as e:
            logger.exception('Error while checking activity compatibility:')
            raise SWFRegistrationError(e)
        r_d_t_l = a_descr.get('defaultTaskList', {}).get('name')
        if r_d_t_l != d_t_l:
            raise SWFRegistrationError(
                'Default task list for %r version %r does not match: %r != %r' %
                (name, version, r_d_t_l, d_t_l))
        r_d_h = a_descr.get('defaultTaskHeartbeatTimeout')
        if r_d_h != d_h:
            raise SWFRegistrationError(
                'Default heartbeat for %r version %r does not match: %r != %r' %
                (name, version, r_d_h, d_h))
        r_d_sch_c = a_descr.get('defaultTaskScheduleToCloseTimeout')
        if r_d_sch_c != d_sch_c:
            raise SWFRegistrationError(
                'Default schedule to close for %r version %r does not match: %r != %r'
                % (name, version, r_d_sch_c, d_sch_c))
        r_d_sch_s = a_descr.get('defaultTaskScheduleToStartTimeout')
        if r_d_sch_s != d_sch_s:
            raise SWFRegistrationError(
                'Default schedule to start for %r version %r does not match: %r != %r'
                % (name, version, r_d_sch_s, d_sch_s))
        r_d_s_c = a_descr.get('defaultTaskStartToCloseTimeout')
        if r_d_s_c != d_s_c:
            raise SWFRegistrationError(
                'Default start to close for %r version %r does not match: %r != %r'
                % (name, version, r_d_s_c, d_s_c))


class SWFWorkflowConfig(SWFConfigMixin, WorkflowConfig):
    """A configuration object suited for Amazon SWF Workflows.

    Use conf_activity and conf_workflow to configure workflow implementation
    dependencies.
    """

    category = 'swf_workflow'  # venusian category used for this type of confs

    def __init__(self,
                 default_task_list=None,
                 default_workflow_duration=None,
                 default_decision_duration=None,
                 default_child_policy=None,
                 rate_limit=64,
                 deserialize_input=None,
                 serialize_result=None,
                 serialize_restart_input=None):
        """Initialize the config object.

        The timer values are in seconds. The child policy should be one fo
        TERMINATE, REQUEST_CANCEL, ABANDON or None.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies pointing to this activity.

        The rate_limit is used to limit the number of concurrent tasks. A value
        of None means no rate limit.
        """
        super(SWFWorkflowConfig, self).__init__(
            deserialize_input, serialize_result, serialize_restart_input)
        self.default_task_list = default_task_list
        self.default_workflow_duration = default_workflow_duration
        self.default_decision_duration = default_decision_duration
        self.default_child_policy = default_child_policy
        self.rate_limit = rate_limit
        self.proxy_factory_registry = {}

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        d_t_l = str_or_none(self.default_task_list)
        d_w_d = timer_encode(self.default_workflow_duration, 'default_workflow_duration')
        d_d_d = timer_encode(self.default_decision_duration, 'default_decision_duration')
        d_c_p = cp_encode(self.default_child_policy)
        return d_t_l, d_w_d, d_d_d, d_c_p

    def try_register_remote(self, swf_layer1, domain, name, version):
        """Register the workflow remotely.

        Returns True if registration is successful and False if another
        workflow with the same name is already registered.

        A name should be set before calling this method or RuntimeError is
        raised.

        Raise SWFRegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.
        """
        d_t_l, d_w_d, d_d_d, d_c_p = self._cvt_values()
        try:
            swf_layer1.register_workflow_type(
                str(domain),
                name=str(name),
                version=str(version),
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
            raise SWFRegistrationError(e)
        return True

    def check_compatible(self, swf_layer1, domain, name, version):
        """Check if the remote config has the same defaults as this one.

        Raise SWFRegistrationError in case of SWF communication errors or
        incompatibility and ValueError if any configuration values can't be
        converted to the required types.
        """
        d_t_l, d_w_d, d_d_d, d_c_p = self._cvt_values()
        try:
            w_descr = swf_layer1.describe_workflow_type(str(domain), str(name), str(version))
            w_descr = w_descr['configuration']
        except SWFResponseError as e:
            logger.exception('Error while checking workflow compatibility:')
            raise SWFRegistrationError(e)
        r_d_t_l = w_descr.get('defaultTaskList', {}).get('name')
        if r_d_t_l != d_t_l:
            raise SWFRegistrationError(
                'Default task list for %r version %r does not match: %r != %r' %
                (name, version, r_d_t_l, d_t_l))
        r_d_d_d = w_descr.get('defaultTaskStartToCloseTimeout')
        if r_d_d_d != d_d_d:
            raise SWFRegistrationError(
                'Default decision duration for %r version %r does not match: %r != %r'
                % (name, version, r_d_d_d, d_d_d))
        r_d_w_d = w_descr.get('defaultExecutionStartToCloseTimeout')
        if r_d_w_d != d_w_d:
            raise SWFRegistrationError(
                'Default workflow duration for %r version %r does not match: %r != %r'
                % (name, version, r_d_w_d, d_w_d))
        r_d_c_p = w_descr.get('defaultChildPolicy')
        if r_d_c_p != d_c_p:
            raise SWFRegistrationError(
                'Default child policy for %r version %r does not match: %r != %r' %
                (name, version, r_d_c_p, d_c_p))

    def conf_activity(self, dep_name, version,
                      name=None,
                      task_list=None,
                      heartbeat=None,
                      schedule_to_close=None,
                      schedule_to_start=None,
                      start_to_close=None,
                      serialize_input=None,
                      deserialize_result=None,
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
        proxy_factory = SWFActivityProxyFactory(
            identity=str(dep_name),
            name=str(name),
            version=str(version),
            task_list=str_or_none(task_list),
            heartbeat=timer_encode(heartbeat, 'heartbeat'),
            schedule_to_close=timer_encode(schedule_to_close, 'schedule_to_close'),
            schedule_to_start=timer_encode(schedule_to_start, 'schedule_to_start'),
            start_to_close=timer_encode(start_to_close, 'start_to_close'),
            serialize_input=serialize_input,
            deserialize_result=deserialize_result,
            retry=retry)
        self.conf_proxy_factory(dep_name, proxy_factory)

    def conf_workflow(self, dep_name, version,
                      name=None,
                      task_list=None,
                      workflow_duration=None,
                      decision_duration=None,
                      child_policy=None,
                      serialize_input=None,
                      deserialize_result=None,
                      retry=(0, 0, 0)):
        """Same as conf_activity but for sub-workflows."""
        if name is None:
            name = dep_name
        proxy_factory = SWFWorkflowProxyFactory(
            identity=str(dep_name),
            name=str(name),
            version=str(version),
            task_list=str_or_none(task_list),
            workflow_duration=timer_encode(workflow_duration, 'workflow_duration'),
            decision_duration=timer_encode(decision_duration, 'decision_duration'),
            child_policy=cp_encode(child_policy),
            serialize_input=serialize_input,
            deserialize_result=deserialize_result,
            retry=retry)
        self.conf_proxy_factory(dep_name, proxy_factory)

    def wrap(self, func):
        """Insert an additional DescCounter object for rate limiting."""
        f = super(SWFWorkflowConfig, self).wrap(func)

        @functools.wraps(func)
        def wrapper(input_data, *extra_args):
            extra_args = extra_args + (DescCounter(int(self.rate_limit)), )
            return f(input_data, *extra_args)

        return wrapper


class SWFRegistrationError(Exception):
    """Can't register a task remotely."""


def cp_encode(val):
    if val is not None:
        val = str(val).upper()
    if val not in ['TERMINATE', 'REQUEST_CANCEL', 'ABANDON', None]:
        raise ValueError('Invalid child policy value: %r' % val)
    return val


def timer_encode(val, name):
    if val is None:
        return None
    val = max(int(val), 0)
    if val == 0:
        raise ValueError(
            'The value of %r must be a strictly positive integer: %r' %
            (name, val))
    return str(val)
