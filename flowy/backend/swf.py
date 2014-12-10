import sys
from keyword import iskeyword

import venusian

__all__ = ['SWFWorkflowConfig', 'SWFWorkflowRegistry', 'RegistrationError']


_CHILD_POLICY = ['TERMINATE', 'REQUEST_CANCEL', 'ABANDON', None]


class SWFWorkflowConfig(object):
    """A configuration object suited for Amazon SWF Workflows.

    Use conf_activity and conf_workflow to configure workflow implementation
    dependencies.
    """

    def __init__(self, version, name=None, default_task_list=None,
                 default_workflow_duration=3600,
                 default_decision_duration=600,
                 default_child_policy='TERMINATE'):
        """Initialize the config object.

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
        self.d_t_l = default_task_list
        self.d_w_d = default_workflow_duration
        self.d_d_d = default_decision_duration
        self.d_c_p = default_child_policy
        self.proxy_factory_registry = {}

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
        klass = self.__class__
        # Make a clone since this config can be used as a decorator on multiple
        # workflow factories and each has a different name.
        c = klass(self._version, name=name,
            default_task_list=self.d_t_l,
            default_workflow_duration=self.d_w_d,
            default_decision_duration=self.d_d_d,
            default_child_policy=self.d_c_p)
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

    def _cvt_name_version(self):
        if self._name is None:
            raise RuntimeError('Name is not set.')
        return str(self._name), str(self._version)

    def _cvt_values(self):
        """Convert values to their expected types or bailout."""
        name, version = self._cvt_name_version()
        d_t_l = _str_or_none(self.d_t_l),
        d_w_d = _timer_encode(self.d_w_d, 'default_workflow_duration')
        d_d_d = _timer_encode(self.d_d_d, 'default_decision_duration')
        d_c_p = _str_or_none(self.d_c_p)
        if child_policy not in _CHILD_POLICY:
            raise ValueError('Invalid child policy value: %r' % d_c_p)
        return name, version, d_t_l, d_w_d, d_d_d, d_c_p

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

    def _check_dep(self, dep_name):
        # stolen from namedtuple
        if not all(c.isalnum() or c=='_' for c in dep_name):
            raise ValueError('Dependency names can only contain alphanumeric characters and underscores: %r' % name)
        if iskeyword(name):
            raise ValueError('Dependency names cannot be a keyword: %r' % name)
        if dep_name[0].isdigit():
            raise ValueError('Dependency names cannot start with a number: %r' % dep_name)
        if dep_name in self.proxy_factory_registry:
            raise ValueError('Dependency name is already registered: %r' % dep_name)

    def conf(self, dep_name, proxy_factory):
        """Configure a proxy factory for a dependency."""
        self._check_dep(dep_name)
        self.proxy_factory_registry[dep_name] = proxy_factory

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
        if name is None:
            name = dep_name
        proxy = SWFActivityProxy(identity=dep_name, name=name, version=version,
                                 task_list=task_list, heartbeat=heartbeat,
                                 schedule_to_close=schedule_to_close,
                                 schedule_to_start=schedule_to_start,
                                 start_to_close=start_to_close)
        self.conf(dep_name, proxy)

    def conf_workflow(self, dep_name, version, name=None, task_list=None,
                      workflow_duration=None, decision_duration=None):
        """Same as conf_activity but for sub-workflows."""
        if name is None:
            name = dep_name
        proxy = SWFWorkflowProxy(identity=dep_name, name=name, version=version,
                                 task_list=task_list,
                                 workflow_duration=workflow_duration,
                                 decision_duration=decision_duration)
        self.conf(dep_name, proxy)

    def bind(self, context):
        """Bind the current configuration to an execution context.

        Returns a callable that can be used to instantiate workflow factories
        passing proxies bound to this execution context.
        """
        d = {}
        for dep_name, proxy in self.proxy_factory_registry.iteritems():
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

    def __repr__(self):
        klass = self.__class__.__name__
        name = self._name if self._name is not None else '__UNNAMED__'
        v = self._version
        MAX_DEPS = 7
        deps = sorted(self.proxy_factory_registry.keys())
        l_deps = len(deps)
        if l_deps > MAX_DEPS:
            deps = deps[:MAX_DEPS] + ['... and %s more' % (l_deps - MAX_DEPS)]
        return '<%s %s v=%s deps=%s>' % (klass, name, v, ','.join(deps))


class SWFWorkflowRegistry(object):
    """A factory for all registered workflows and their configs.

    Register and/or detect workflows and their configuration. The registered
    workflows can be identified by their name and version and instantiated by
    the context bound config objects.
    """
    def __init__(self):
        self.registry = {}

    def register(self, config, workflow_factory):
        """Register a configuration and a workflow factory.

        It's an error to register equivalent configs twice. An equivalent
        config is one that has the same hash and is equal to another.
        For SWFWorkflowConfig this means the same name and version.
        """
        config = config.set_alternate_name(workflow_factory.__name__)
        if config in self.registry:
            raise ValueError('Config is already registered: %r' % config)
        self.registry[config] = (config, workflow_factory)

    def register_remote(self, layer1):
        """Register or check compatibility of all configs in Amazon SWF."""
        for config in self.registry.keys():
            config.register(layer1)

    def __call__(self, name, version, context):
        """Bind the corresponding config to the context and init a workflow.

        Raise value error if no config is found for this name and version,
        otherwise bind the config to the context and use it to instantiate the
        workflow.
        """
        # Convert to str since that's what the config is expecting
        key = (str(name), str(version))
        try:
            config, workflow_factory = self.registry[key]
        except KeyError:
            raise ValueError('No config with the name %r and version %r was found.' % (name, version))
        return config.bind(context)(workflow_factory)

    def scan(self, package=None, ignore=None, level=0):
        """Scan for registered workflows and their configuration.

        Use venusian to scan. By default it will scan the package of the scan
        caller but this can be changed using the package and ignore arguments.
        Their semantics is the same with the ones in venusian documentation.

        The level represents the additional stack frames to add to the caller
        package identification code. This is useful when this call is wrapped
        in another place like so:

            def scan():
                scanner = SWFWorkflowRegistry()
                scanner.scan(level=1)
                return scanner

            # ... and later, in another package
            my_scanner = scan()
        """
        scanner = venusian.Scanner(register=self.register)
        if package is None:
            package = caller_package(level=2 + level)
        scanner.scan(package, categories=['swf_workflow'], ignore=ignore)

    def __repr__(self):
        klass = self.__class__.__name__
        configs = self.registry.values()
        MAX_ENTRIES = 4
        entries = sorted(self.registry.values())
        l_entries = len(entries)
        r = '<%s %r>'
        if l_entries > MAX_ENTRIES:
            entries = entries[:MAX_ENTRIES]
            extra_entries = l_entries - MAX_ENTRIES
            return '<%s %r ... and %s more>' % (klass, entries, extra_entries)
        return '<%s %r>' % (klass, entries)


class SWFActivityProxy(object):
    def __init__(self, identity, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None):
        self.identity = identity
        self.name = name
        self.version = version
        self.task_list = task_list
        self.heartbeat = heartbeat
        self.schedule_to_close = schedule_to_close
        self.schedule_to_start = schedule_to_start
        self.start_to_close = start_to_close

    def bind(self, context):
        return SWFBoundProxy(self, context)


class SWFWorkflowProxy(object):
    def __init__(self, identity, name, version, task_list=None,
                 workflow_duration=None, decision_duration=None):
        self.identity = identity
        self.name = name
        self.version = version
        self.task_list = task_list
        self.workflow_duation = workflow_duration
        self.decision_duation = decision_duration

    def bind(self, context):
        return SWFBoundProxy(self, context)


class SWFBoundProxy(object):
    def __init__(self, proxy, context):
        self.proxy = proxy
        self.context = context


class RegistrationError(Exception):
    pass


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


# Stolen from Pyramid
def caller_module(level=2, sys=sys):
    module_globals = sys._getframe(level).f_globals
    module_name = module_globals.get('__name__') or '__main__'
    module = sys.modules[module_name]
    return module


def caller_package(level=2, caller_module=caller_module):
    # caller_module in arglist for tests
    module = caller_module(level+1)
    f = getattr(module, '__file__', '')
    if (('__init__.py' in f) or ('__init__$py' in f)):  # empty at >>>
        # Module is a package
        return module  # pragma: no cover
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
