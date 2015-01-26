import logging
import sys
from collections import namedtuple
from functools import partial
from keyword import iskeyword

import venusian

__all__ = 'restart TaskError TaskTimedout wait_first wait_n wait_all'.split()


logger = logging.getLogger(__package__)


_identity = lambda x: x
_serialize_args = lambda *args, **kwargs: (args, kwargs)


class Activity(object):
    """A generic activity configuration object."""

    category = None

    def __init__(self, deserialize_input=_identity,
                 serialize_result=_identity):
        """Initialize the activity config object.

        The deserialize_input/serialize_result callables are used to
        deserialize the initial input data and serialize the final result.
        By default they are the identity functions.
        """
        self.deserialize_input = deserialize_input
        self.serialize_result = serialize_result

    def __call__(self, obj):
        """Associate an object to this config and make it discoverable.

        The config object can be used as a decorator to bind it to an object
        and make it discoverable later using a scanner. The original object is
        preserved.

            mycfg = MyConfig(version=1)

            @mycfg
            def x(..):
                ...

            # ... and later
            scanner.scan()
        """
        def callback(venusian_scanner, *_):
            """This gets called by venusian at scan time."""
            venusian_scanner.register(self, obj)
        venusian.attach(obj, callback, category=self.category)
        return obj

    init = partial

    @property
    def key(self):
        """A unique identifier for this config used for registration."""
        return self


class Workflow(Activity):
    """A generic workflow configuration object with dependencies."""

    def __init__(self, deserialize_input=_identity,
                 serialize_result=_identity,
                 serialize_restart_input=_serialize_args):
        """Initialize the workflow config object.

        The deserialize_input, serialize_result and serialize_restart_input
        callables are used to deserialize the initial input data, serialize the
        final result and serialize the restart arguments.
        """
        super(Workflow, self).__init__(deserialize_input, serialize_result)
        self.serialize_restart_input = serialize_restart_input
        self.proxy_factory_registry = {}

    def _check_dep(self, dep_name):
        """Check if dep_name is a unique valid identifier name."""
        # stolen from namedtuple
        if not all(c.isalnum() or c == '_' for c in dep_name):
            raise ValueError('Dependency names can only contain alphanumeric characters and underscores: %r' % dep_name)
        if iskeyword(dep_name):
            raise ValueError('Dependency names cannot be a keyword: %r' % dep_name)
        if dep_name[0].isdigit():
            raise ValueError('Dependency names cannot start with a number: %r' % dep_name)
        if dep_name in self.proxy_factory_registry:
            raise ValueError('Dependency name is already registered: %r' % dep_name)

    def conf_proxy(self, dep_name, proxy_factory):
        """Set a proxy factory for a dependency name."""
        self._check_dep(dep_name)
        self.proxy_factory_registry[dep_name] = proxy_factory

    def init(self, impl_factory, *args, **kwargs):
        """Instantiate the workflow factory object.

        Call each proxy with *args and **kwargs and instatiate the workflow
        factory with the results.
        """
        wf_kwargs = {}
        for dep_name, proxy in self.proxy_factory_registry.iteritems():
            wf_kwargs[dep_name] = proxy(*args, **kwargs)
        return impl_factory(**wf_kwargs)

    def __repr__(self):
        klass = self.__class__.__name__
        deps = sorted(self.proxy_factory_registry.keys())
        max_entries = 5
        more_deps = len(deps) - max_entries
        if more_deps > 0:
            deps = deps[:max_entries] + ['... and %s more' % more_deps]
        return '<%s deps=%s>' % (klass, ','.join(deps))


class Worker(object):
    """A runner for all registered task implementations and their configs."""

    categories = []  # venusian categories to scan for

    def __init__(self):
        self.registry = {}

    def register(self, config, impl_factory):
        """Register a task configuration and an implementation."""
        key = config.key
        if key in self.registry:
            raise ValueError('Implementation is already registered: %r' % key)
        self.registry[key] = (config, impl_factory)

    def __call__(self, key, input_data, *args, **kwargs):
        if key not in self.registry:
            logger.error("Colud not find implementation for key: %r", key)
            return
        config, impl_factory = self.registry[key]
        impl = config.init(impl_factory, *args, **kwargs)
        deserialize_input = getattr(config, 'deserialize_input', _identity)
        try:
            iargs, ikwargs = deserialize_input(input_data)
        except Exception:
            logger.exception('Error while deserializing input:')
            raise
        result = impl(*iargs, **ikwargs)
        if isinstance(result, _restart): # If it's a restart return it
            sri = getattr(config, 'serialize_restart_input', _identity)
            try:
                input_data = sri(*result.args, **result.kwargs)
            except Exception:
                logger.exception('Error while serializing restart input:')
                raise
            raise Restart(input_data)
        serialize_result = getattr(config, 'serialize_result', _identity)
        try:
            return serialize_result(result)
        except Exception:
            logger.exception('Error while serializing result:')
            raise

    def scan(self, package=None, ignore=None, level=0):
        """Scan for registered workflows and their configuration.

        Use venusian to scan. By default it will scan the package of the scan
        caller but this can be changed using the package and ignore arguments.
        Their semantics is the same with the ones in venusian documentation.

        The level represents the additional stack frames to add to the caller
        package identification code. This is useful when this call is wrapped
        in another place like so:

            def scan():
                reg = WorkflowRegistry()
                reg.scan(level=1)
                return reg

            # ... and later, in another package
            reg = scan()
        """
        scanner = venusian.Scanner(register=self.register)
        if package is None:
            package = _caller_package(level=2 + level)
        scanner.scan(package, categories=self.categories, ignore=ignore)

    def __repr__(self):
        klass = self.__class__.__name__
        max_entries = 5
        entries = sorted(self.registry.values())
        more_entries = len(entries) - max_entries
        if more_entries > 0:
            entries = entries[:max_entries]
            return '<%s %r ... and %s more>' % (klass, entries, more_entries)
        return '<%s %r>' % (klass, entries)


class ContextBoundProxy(object):
    """A proxy bound to a context.

    This is what gets passed as a dependency in a workflow and has most of the
    scheduling logic. The real scheduling is dispatched to the proxy; this
    logic can be reused across different backends.
    """
    def __init__(self, proxy, context):
        self.proxy = proxy
        self.context = context
        self.call_number = 0

    def _call_key(self, retry_number):
        r = "%s-%s-%s" % (self.proxy.identity, self.call_number, retry_number)
        self.call_number += 1
        return r

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
        context = self.context
        result = Placeholder()
        retry = getattr(self.proxy, 'retry', [0])
        for retry_number, delay in enumerate(retry):
            call_key = self._call_key(retry_number)
            if context.is_timeout(call_key):
                continue
            if context.is_running(call_key):
                break
            if context.is_result(call_key):
                value, order = context.result(call_key)
                # Make the result deserialization lazy; in case of
                # deserialization errors the result will fail the workflow
                d_r = getattr(self.proxy, 'deserialize_result', _identity)
                d_r = partial(d_r, value)
                result = Result(context, d_r, order)
                break
            if context.is_error(call_key):
                err, order = context.error(call_key)
                result = Error(err, order)
                break
            errors, placeholders = _short_circuit_on_args(args, kwargs)
            if errors:
                result = wait_first(errors)
            elif not placeholders:
                if not self.rate_limit.consume():
                    # Enough tasks have been scheduled for this decision
                    break
                try:
                    # This can fail if a result can't deserialize.
                    a, kw = _extract_results(args, kwargs)
                except SuspendTask:
                    # In this case the result will fail the workflow and
                    # raise SuspendTask to act as a Placeholder.
                    # If that's the case return a Placeholder since the
                    # workflow was already failed.
                    break
                # really schedule
                try:
                    # Let the proxy serialize the args as there might be
                    # other things (like timers) than need to be scheduled
                    # before the real task is scheduled
                    self.proxy.schedule(context, call_key, delay, *a, **kw)
                except Exception as e:
                    # If there are (input serialization) errors, fail the
                    # workflow and pretend the task is running
                    logger.exception('Cannot schedule task:')
                    context.fail(e)
            break
        else:
            # No retries left, it must be a timeout
            order = context.timeout(call_key)
            result = Timeout(order)
        return result

    def __repr__(self):
        klass = self.__class__.__name__
        return "<%s %r %r>" % (klass, self.context, self.proxy)


class TaskResult(object):
    """Base class for all different types of task results."""
    _order = None

    def __lt__(self, other):
        if not isinstance(other, TaskResult):
            return NotImplemented
        if self._order is None:
            return False
        if other._order is None:
            return True
        return self._order < other._order

    def result(self):
        """Get the deserialized result of the task.

        This method can raise 3 different types of exceptions:
        * TaskError: if the task failed for whatever reason. This usually means
          the task implementation raised an unhandled exception.
        * TaskTimedout - If the task timed-out on all retry attemps.
        * SuspendTask - This is an internal exception used by Flowy as control
          flow and should not be handled by user code.
        """
        raise NotImplementedError

    def wait(self):
        """Wait for a task to complete.

        This method may raise SuspendTask. See .result() for more details.
        """
        raise NotImplementedError

    def is_error(self):
        """Test if this result represents an error."""
        return False


class Result(TaskResult):
    """The result of a finished task.

    A note about this class: even if .wait() doesn't block (by raising
    SuspendTask) this doesn't guarantee that .result() call won't block.
    Actually, if the result can't be deserialized this class will act as if
    it's a placeholder when calling .result().
    """
    def __init__(self, context, lazy_result, order):
        self._context = context
        self._lazy_result = lazy_result
        self._order = order

    def result(self):
        if not hasattr(self, '_result_cache'):
            try:
                self._result_cache = self._lazy_result()
            except Exception as e:
                logger.exception('Error while deserializing result:')
                self._result_cache = e
                self._context.fail(e)
        if isinstance(self._result_cache, Exception):
            # Act as if the result is not available
            raise SuspendTask
        else:
            return self._result_cache

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


def wait_first(result, *results):
    """Return the result of the first task to finish from a list of results.

    If no task is finished yet it can raise SuspendTask.
    """
    return min(_i_or_args(result, results)).wait()


def wait_n(n, result, *results):
    """Wait for first n tasks to finish and return their results in order.

    This is a generator yielding results in the order their tasks finished. If
    more results are consumed from this generator than tasks finished it will
    raise SuspendTask. This means that you can use it to access results as soon
    as possible, even before all n tasks are finished.
    """
    i = _i_or_args(result, results)
    if n == 1:
        yield wait_first(i)
        return
    for result in sorted(i)[:n]:
        yield result.wait()


def wait_all(result, *results):
    """Wait for all the tasks to finish and return their results in order.

    Works just like wait_n(len(x), x)
    """
    i = list(_i_or_args(result, results))
    for result in wait_n(len(i), i):
        yield result


def _i_or_args(result, results):
    if len(results) == 0:
        return iter(result)
    return (result,) + results


class SuspendTask(Exception):
    """Special exception raised by result and used for flow control."""


class TaskError(Exception):
    """Raised by result when a task failed its execution."""


class TaskTimedout(TaskError):
    """Raised by result when a task has timedout its execution."""


class Restart(Exception):
    """Raised if the workflow returns a restart."""
    def __init__(self, input_data):
        self.input_data = input_data


def _short_circuit_on_args(a, kw):
    args = a + tuple(kw.values())
    errs, placeholders = [], False
    for result in args:
        if isinstance(result, TaskResult):
            try:
                if result.is_error():
                    errs.append(result)
            except SuspendTask:
                placeholders = True
    return errs, placeholders


def _extract_results(a, kw):
    aa = [_result_or_value(r) for r in a]
    kwkw = dict((k, _result_or_value(v)) for k, v in kw.iteritems())
    return aa, kwkw


def _result_or_value(result):
    if isinstance(result, TaskResult):
        return result.result()
    return result


_restart = namedtuple('restart', 'args kwargs')
def restart(*args, **kwargs):
    """Return an instance of this to restart a workflow with the new input."""
    return _restart(args, kwargs)


def setup_default_logger():
    """Configure the default logger for Flowy."""
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s %(levelname)s\t%(name)s: %(message)s'
            }},
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'simple'
            }},
        'loggers': {
            'flowy': {
                'handlers': ['console'],
                'popagate': False,
                'level': 'INFO',
            }}
    })


# Stolen from Pyramid
def _caller_module(level=2, sys=sys):
    module_globals = sys._getframe(level).f_globals
    module_name = module_globals.get('__name__') or '__main__'
    module = sys.modules[module_name]
    return module


def _caller_package(level=2, caller_module=_caller_module):
    # caller_module in arglist for tests
    module = caller_module(level+1)
    f = getattr(module, '__file__', '')
    if (('__init__.py' in f) or ('__init__$py' in f)):  # empty at >>>
        # Module is a package
        return module  # pragma: no cover
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
