from contextlib import contextmanager

from flowy.proxy import _sentinel
from flowy.proxy import TaskProxy
from flowy.task import Task
from flowy.task import Workflow



_CHILD_POLICY = ['TERMINATE', 'REQUEST_CANCEL', 'ABANDON', None]


class RegistrationError(Exception):
    pass


class SWFWorkflowConfig(object):
    """A configuration object suited for Amazon SWF Workflows.

    Use conf_activity and conf_workflow to configure workflow implementation
    dependencies.
    """

    def __init__(version, name=None, default_task_list=None,
                 default_workflow_duration=None,
                 default_decision_duration=120,
                 default_child_policy='TERMINATE'):
        """ Initialize the config object.

        The timer values are in seconds, and the child policy should be either
        TERMINATE, REQUEST_CANCEL, ABANDON or None.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies.

        The name is not required at this point but should be set before trying
        to register this config remotely and can be set later with
        set_alternate_name.
        """
        # The name and the version are used to compute the hash for this config
        # and should be treated as immutable
        self._name = name
        self._version = version
        self.default_task_list = task_list
        self.default_workflow_duration = default_workflow_duration
        self.default_decision_duration = default_decision_duration
        self.default_child_policy = default_child_policy
        self._task_registry = {}

    def set_alternate_name(self, name):
        """Set the name of this workflow if one is not already set.

        Returns a configuration instance with the new name or the existing
        instance if the name was not changed.

        It's useful to return a new instance because if this config lacks a
        name it can be used to register multiple factories and fallback to each
        factory __name__ value.
        """
        if self._name is not None:
            return self
        # Make a clone for each alternate name
        return SWFWorkflowConfig(self._version, name=name,
            default_task_list=self.default_task_list,
            default_workflow_duration=self.default_workflow_duration,
            default_decision_duration=self.default_decision_duration,
            default_child_policy=self.default_child_policy)

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
        registered_as_new = self._register_remote(swf_layer1)
        if not registered_as_new:
            success = self._check_compatible(swf_layer1)

    def _cvt_name_version(self):
        if self._name is None:
            raise RuntimeError('Name is not set.' % self)
        return str(self._name), str(self._version)

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        name, version = self._cvt_name_version()
        t_list = _str_or_none(self.default_task_list),
        workflow_dur = _timer_encode('default_workflow_duration',
                                          self.default_workflow_duration)
        decision_dur = _timer_encode('default_decision_duration',
                                          self.default_decision_duration)
        child_policy = _str_or_none(self.default_child_policy)
        if child_policy not in _CHILD_POLICY:
            raise ValueError("Invalid child policy value.")
        return name, version, t_list, workflow_dur, decision_dur, child_policy

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
        name, ver, t_list, wf_dur, d_dur, child_pol = self._cvt_values()
        try:
            swf_layer1.register_workflow_type(
                name=name, version=ver, task_list=t_list,
                default_task_start_to_close_timeout=d_dur,
                default_execution_start_to_close_timeout=wf_dur,
                default_child_policy=child_pol)
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
        name, ver, t_list, wf_dur, d_dur, child_pol = self._cvt_values()
        try:
            w = swf_layer1.describe_workflow_type(
                workflow_name=name, workflow_version=ver)['configuration']
        except SWFResponseError as e:
            logger.exception('Error while checking workflow compatibility:')
            raise RegistrationError(e)
        return (
            w.get('defaultTaskList', {}).get('name') == t_list
            and w.get('defaultTaskStartToCloseTimeout') == d_dur
            and w.get('defaultExecutionStartToCloseTimeout') == wf_dur
            and w.get('defaultChildPolicy') == child_pol)

    def _check_dep(self, dep_name):
        if dep_name in self._task_registry:
            raise ValueError("%r is already configured.")

    def conf_activity(self, dep_name, version, name=None, task_list=None,
                      heartbeat=None, schedule_to_close=None,
                      schedule_to_start=None, start_to_close=None):
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
        self._check_dep(dep_name)
        if name is None:
            name = dep_name
        proxy = SWFActivityProxy(identity=dep_name, name=name, version=version,
                                 task_list=task_list, heartbeat=heartbeat,
                                 schedule_to_close=schedule_to_close,
                                 schedule_to_start=schedule_to_start,
                                 start_to_close=start_to_close)
        self._task_registry[dep_name] = proxy

    def conf_workflow(self, dep_name, version, name=None, task_list=None,
                      workflow_duration=None, decision_duration=None):
        """Same as conf_activity but for sub-workflows."""
        self._check_dep(dep_name)
        if name is None:
            name = dep_name
        proxy = SWFWorkflowProxy(identity=dep_name, name=name, version=version,
                                 task_list=task_list,
                                 workflow_duration=workflow_duration,
                                 decision_duration=decision_duration)
        self._task_registry[dep_name] = proxy

    def bind(self, context):
        """Bind the current configuration to an execution context.

        Returns a callable that can be used to instantiate workflow factories
        passing proxies bound to this execution context.
        """
        d = {}
        for dep_name, proxy in self._task_registry.iteritems():
            d[dep_name] = proxy.bind(context)
        return lambda wf_factory: wf_factory(**d)

    def __eq__(self, other):
        """Compare another config or a (name, version) tuple with self."""
        if isinstance(other, SWFWorkflowConfig):
            return self._cvt_name_version() == other._cvt_name_version()
        try:
            name, version = other
        except Exception:
            return NotImplemented
        return self._cvt_name_version() == (name, version)

    def __hash__(self):
        return hash(self._cvt_name_version())

    def __call__(self, workflow_factory):
        """Associate the factory to this config and make it discoverable.

        The config object can be used as a decorator to bind it to a workflow
        factory and make it discoverable later using a scanner. The original
        factory is preserved.

            cfg = SWFWorkflowConfig(version=1)
            cfg.conf_activity('a', name='MyActivity', version=1)
            cfg.conf_activity('b', version=2, task_list='my_tl')

            @cfg
            class MyWorkflow:
                def __init__(self, a, b):
                    pass

            # ... and later
            scanner.scan()

        If this config doesn't have a name set, it can be used as a decorator
        on multiple factories and will use each factory __name__ value as its
        name.
        """
        def callback(venusian_scanner, f_name, obj):
            config = self.set_alternate_name(f_name)
            venusian_scanner.register(config, workflow_factory)
        venusian.attach(workflow_factory, callback, category='swf_workflow')
        return workflow_factory


class SWFWorkflowRegistry(object):
    def __init__(self):
        self._registry = {}

    def register(self, config, workflow_factory):
        """Register a configuration and a workflow factory."""
        config = config.set_alternate_name(workflow_factory.__name__)
        config.register()  # Can raise, let it bubble up and stop this.
        self._registry[config] = (config, workflow_factory)

    def __call__(self, config_identity, context):
        """Bind the config to context and instantiate the workflow factory."""
        config, workflow_factory = self._registry[config_identity]
        factory_DI = config.bind(context)
        return factory_DI.instantiate(workflow_factory)

    def scan(self):
        pass


def _timer_encode(val, name):
    if val is None:
        return None
    val = max(int(val), 0)
    if val == 0:
        raise ValueError("The value of %r must be a strictly"
                         " positive integer." % name)
    return str(val)


def _str_or_none(val):
    if val is None:
        return None
    return str(val)
