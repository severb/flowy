import json
import sys
from functools import partial
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
        self.name = name
        self.version = version
        self.d_t_l = default_task_list
        self.d_w_d = default_workflow_duration
        self.d_d_d = default_decision_duration
        self.d_c_p = default_child_policy
        self.proxy_factory_registry = {}
        self.rate_limit = rate_limit
        self.deserialize_input = deserialize_input
        self.serialize_result = serialize_result

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

    def _cvt_name_version(self):
        if self.name is None:
            raise RuntimeError('Name is not set.')
        return str(self.name), str(self.version)

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

    def bind(self, context):
        """Bind the current configuration to an execution context.

        Returns a callable that can be used to instantiate workflow factories
        passing proxies bound to this execution context.
        """
        rate_limit = _DescCounter(self.rate_limit)
        d = {}
        for dep_name, proxy in self.proxy_factory_registry.iteritems():
            d[dep_name] = proxy.bind(context, rate_limit)
        return lambda wf_factory: wf_factory(**d)

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
            venusian_scanner.register(config, workflow_factory)
        venusian.attach(workflow_factory, callback, category='swf_workflow')
        return workflow_factory

    def __repr__(self):
        klass = self.__class__.__name__
        name = self.name if self.name is not None else '__UNNAMED__'
        v = self.version
        MAX_DEPS = 7
        deps = sorted(self.proxy_factory_registry.keys())
        l_deps = len(deps)
        if l_deps > MAX_DEPS:
            deps = deps[:MAX_DEPS] + ['... and %s more' % (l_deps - MAX_DEPS)]
        return '<%s %s v=%s deps=%s>' % (klass, name, v, ','.join(deps))


class SWFWorkflow(object):
    """Bind a config and a workflow factory together.

    This will set the config alternate name to the workflow factory __name__.
    """
    def __init__(self, config, workflow_factory):
        self.config = config.set_alternate_name(workflow_factory.__name__)
        self.workflow_factory = workflow_factory
        self.register = config.register  # delegate

    @property
    def name(self):
        return self.config.name

    @property
    def version(self):
        return self.config.version

    def run(self, context, input):
        """Run the workflow code.

        Bind the config to the context and use it to instantiate and run a new
        workflow instance.
        """
        workflow_DI = config.bind(context)
        wf = workflow_DI(self.workflow_factory)
        try:
            deserialize_input = getattr(config, 'deserialize_input', _i)
            args, kwargs = deserialize_input(input)
        except Exception as e:
            logger.exception('Error while deserializing workflow input:')
            context.fail(e)
            return
        try:
            r = wf.run(*args, **kwargs)
        except SuspendTask:
            context.flush()
        except Exception as e:
            logger.exception('Error while running:')
            context.fail(e)
        else:
            if isinstance(r, _restart):
                context.restart(*r.args, **r.kwargs)
            else:
                try:
                    serialize_result = getattr(config, 'serialize_input', _i)
                    r = serialize_result(r)
                except Exception as e:
                    logger.exception('Error while serializing workflow result:')
                    context.fail(e)
                else:
                    context.finish(r)

    def __repr__(self):
        klass = self.__class__.__name__
        return "<%s %r %r>" % (klass, self.config, self.workflow_factory)


class SWFWorkflowRegistry(object):
    """A factory for all registered workflows and their configs.

    Register and/or detect workflows and their configuration. The registered
    workflows can be identified by their name and version and run by passing
    a context and the input arguments.
    """
    def __init__(self):
        self.registry = {}

    def _key(self, name, version):
        return (str(name), str(version))

    def register(self, config, workflow_factory):
        """Register a configuration and a workflow factory.

        It's an error to register the same name and version twice.
        """
        workflow = SWFWorkflow(config, workflow_factory)
        key = self._key(workflow.name, workflow.version)
        if key in self.registry:
            raise ValueError('Implementation is already registered: %r' % key)
        self.registry[key] = workflow

    def register_remote(self, layer1):
        """Register or check compatibility of all configs in Amazon SWF."""
        for workflow in self.registry.keys():
            workflow.register(layer1)

    def __call__(self, name, version, context, *args, **kwargs):
        """Bind the corresponding config to the context and init a workflow.

        Raise value error if no config is found for this name and version,
        otherwise bind the config to the context and use it to instantiate the
        workflow.
        """
        key = self._key(name, version)
        try:
            workflow = self.registry[key]
        except KeyError:
            raise ValueError('No workflow implementation found: %r' % key)
        workflow.run(context, *args, **kwargs)

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


class ContextBoundProxy(object):
    """A proxy bound to a context.

    This is what gets passed as a dependency in a workflow and has most of the
    scheduling logic. The real scheduling is dispatched back to the proxy; this
    way this logic can be reused across different backends.
    """
    def __init__(self, proxy, context, rate_limit=_DescCounter()):
        self.proxy = proxy
        self.context = context
        self.rate_limit = rate_limit

    def _call_key(self, call_number):
        return "%s-%s" % (self.proxy.identity, call_number)

    def __call__(self, *args, **kwargs):
        """Consult the execution history for results or schedule a new task.

        This is method gets called from the user workflow code.
        When calling it, the task it refers to can be in one of the following
        states: RUNNING, READY, FAILED, TIMEDOUT or NOTSCHEDULED.

        * If the task is RUNNING this returns a Placeholder. The Placeholder
          interrupts the workflow execution if its result is accessed by
          raising a SuspendTask exception.
        * If the task is READY this returns a Result object. Calling the result
          method on this object will just return the final value the task
          produced.
        * If the task is FAILED this returns an Error object. Calling the
          result method on this object will raise a TaskError exception
          containing the error message set by the task.
        * In case of a TIMEOUT this returns an Timeout object. Calling the
          result method on this object will raise TaskTimedout exception, a
          subclass of TaskError.
        * If the task was NOTSCHEDULED yet:
            * If any errors in arguments, propagate the error by returning
              another error.
            * If any placeholders in arguments, don't do anything because there
              are unresolved dependencies.
            * Finally, if all the arguments look OK, extract the values from
              any result objects that might be in the arguments and schedule it
              for execution.
        """
        c = self.context
        r = Placeholder()
        retry = getattr(self.proxy, 'retry', [0])
        for call_number, delay in enumerate(retry):
            call_key = self._call_key(call_number)
            if c.timeout(call_key):
                continue
            if c.running(call_key):
                break
            if c.has_result(call_key):
                order, value = c.result(call_key)
                # make the result deserialization lazy; in case of
                # deserialization errors the result will fail the workflow
                d_r = getattr(self.proxy, 'deserialize_result', _i)
                d_r = partial(d_r, value)
                r = Result(context, d_r, order)
                break
            if c.has_error(call_key):
                order, err = c.error(call_key)
                r = Error(err, order)
                break
            errors, placeholders = _short_circuit_on_args(args, kwargs)
            if errors:
                r = first(errors)
            elif not placeholders:
                if self.rate_limit.consume():
                    try:
                        # This can fail if a result can't deserialize.
                        args, kwargs = _extrac_results(args, kwargs)
                    except SuspendTask:
                        # In this case the resut will fail the workflow and
                        # pretend the task running by returning a placehoder
                        break
                    # really schedule
                    self.proxy.schedule(context, call_key, delay, args, kwargs)
            break
        else:
            # no retries left, it must be a timeout
            assert c.has_timeout(call_key), 'No timeout for: %r' % call_key
            order = c.timeout(call_key)
            r = Timeout(order)
        return r

    def __repr__(self):
        klass = self.__class__.__name__
        return "<%s %r %r>" % (klass, self.context, self.proxy)



class TaskResult(object):
    _order = None

    def __lt__(self, other):
        if not isinstance(other, TaskResult):
            return NotImplemented
        if self._order is None:
            return False
        if other._order is None:
            return True
        return self._order < other._order


class Result(TaskResult):
    """The result of a finished task.

    A note about this class: even if wait/is_error don't block (by raising
    SuspendTask) this doesn't guarantee that result call won't block.
    Actually, if the result can't be deserialized this class will act as if
    it's a placeholder when calling result.
    """

    def __init__(self, context, lazy_result, order):
        self._context = context
        self._lazy_result = lazy_result
        self._order = order

    def result(self):
        if not hasattr(self, '_result_cache'):
            try:
                self._result_cache = lazy_result()
            except Exception as e:
                logger.exception('Error while deserializing result:')
                self._result_cache = e
                self._context.fail(e)
        if isinstance(self._result_cache, Exception):
            # Act as if the result is not available
            raise SuspendTask
        else:
            return self._result_cache

    def is_error(self):
        return False

    def wait(self):
        return self


class Error(Result):
    def __init__(self, err, order):
        self._err = err
        self._order = order

    def result(self):
        raise TaskError(self._err)

    def is_error(self):
        return True


class Timeout(Error):
    def __init__(self, order):
        self._order = order

    def result(self):
        raise TaskTimedout


class Placeholder(TaskResult):
    def result(self):
        raise SuspendTask

    def is_error(self):
        raise SuspendTask

    def wait(self):
        raise SuspendTask


class SuspendTask(Exception):
    """Special exception raised by result and used for flow control."""


class TaskError(Exception):
    """Raised by result when a task failed its execution."""


class TaskTimedout(TaskError):
    """Raised by result when a task has timedout its execution."""


def _short_circuit_on_args(a, kw):
    args = a + tuple(kw.values())
    errs, placeholders = [], False
    for r in args:
        if isinstance(r, TaskResult):
            try:
                if r.is_error():
                    errs.append(r)
            except SuspendTask:
                placeholders = True
    return errs, placeholders


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


class _DescCounter(object):
    def __init__(self, to=None):
        if to is None:
            self.r = repeat(True)
        else:
            self.r = chain(repeat(True, to), repeat(False))

    def consume(self):
        return next(self.r)


def _s_i(*args, **kwargs):
    return json.dumps((args, kwargs))


_restart = namedtuple('restart', 'args kwargs')
def restart(*args, **kwargs):
    return _restart(args, kwargs)


_d_i = json.loads
_s_r = json.dumps
_d_r = json.loads
_i = lambda x: x


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
