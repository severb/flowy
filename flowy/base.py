import heapq
import logging
import sys
from collections import namedtuple
from functools import partial
from keyword import iskeyword

import venusian

__all__ = 'restart TaskError TaskTimedout wait_first wait_n wait_all parallel_reduce'.split()


logger = logging.getLogger(__package__)


_identity = lambda x: x
_serialize_input = lambda *args, **kwargs: (args, kwargs)
_sentinel = object()


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

    @property
    def key(self):
        """A unique identifier for this config used for registration."""
        return self

    def init(self, activity_impl, decision, *args, **kwargs):
        return partial(activity_impl, decision.heartbeat)


class Workflow(Activity):
    """A generic workflow configuration object with dependencies."""

    def __init__(self, deserialize_input=_identity,
                 serialize_result=_identity,
                 serialize_restart_input=_serialize_input):
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
        # XXX: Introspect and make sure the arguments match
        self._check_dep(dep_name)
        self.proxy_factory_registry[dep_name] = proxy_factory

    def init(self, workflow_factory, decision, *args, **kwargs):
        """Instantiate the workflow factory object.

        Call each proxy with *args and **kwargs and instatiate the workflow
        factory with the results.
        """
        wf_kwargs = {}
        for dep_name, proxy in self.proxy_factory_registry.items():
            wf_kwargs[dep_name] = proxy(decision, *args, **kwargs)
        return workflow_factory(**wf_kwargs)

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

    def register(self, config, impl):
        """Register a task configuration and an implementation.

        Implementations can be executed later by calling Worker instances and
        passing them a key. The key must have the same value the config.key had
        at registration time.
        """
        key = config.key
        if key in self.registry:
            raise ValueError('Implementation is already registered: %r' % key)
        self.registry[key] = (config, impl)

    def __call__(self, key, input_data, decision, *args, **kwargs):
        """Execute the implementation identified by key passing the input_data.

        The associated config is used to instantiate (config.init) the
        implementation passing any *args and **kwargs.
        The implementation instance is then called passing the deserialized
        input_data.

        The actual actions are dispatched to the decision object and can be one
        of:
            * flush() - nothing to do, any pending actions should be commited
            * fail(e) - ignore pending actions, fail the execution
            * finish(e) - ignore pending actions, complete the execution
            * restart(input) - ignore pending actions, restart the execution
        """
        if key not in self.registry:
            logger.error("Colud not find implementation for key: %r", key)
            return  # Let it timeout
        config, impl = self.registry[key]
        try:
            # Pass the decision and any other arguments
            impl = config.init(impl, decision, *args, **kwargs)
        except SuspendTask:
            decision.flush()
        except Exception as e:
            logger.exception("Error while running task:")
            decision.fail(e)
        else:
            try:
                iargs, ikwargs = config.deserialize_input(input_data)
            except Exception as e:
                logger.exception('Error while deserializing input:')
                decision.fail(e)
            else:
                try:
                    result = impl(*iargs, **ikwargs)
                except SuspendTask:
                    decision.flush()
                else:
                    if isinstance(result, _restart):
                        serialize_restart = getattr(
                            config, 'serialize_restart_input', _identity)
                        try:
                            r_i = serialize_restart(*result.args,
                                                    **result.kwargs)
                        except Exception as e:
                            decision.fail(e)
                        else:
                            decision.restart(r_i)
                    else:
                        try:
                            result = config.serialize_result(result)
                        except Exception as e:
                            logger.exception('Error while serializing result:')
                            decision.fail(e)
                        else:
                            decision.finish(result)

    def scan(self, categories=None, package=None, ignore=None, level=0):
        """Scan for registered implementations and their configuration.

        The categories can be used to scan for only a subset of tasks. By
        default it will use the categories property set on the class.

        Use venusian to scan. By default it will scan the package of the scan
        caller but this can be changed using the package and ignore arguments.
        Their semantics is the same with the ones in venusian documentation.

        The level represents the additional stack frames to add to the caller
        package identification code. This is useful when this call happens
        inside another function.
        """
        if categories is None:
            categories = self.categories
        scanner = venusian.Scanner(register=self.register)
        if package is None:
            package = _caller_package(level=2 + level)
        scanner.scan(package, categories=categories, ignore=ignore)

    def __repr__(self):
        klass = self.__class__.__name__
        max_entries = 5
        entries = sorted(self.registry.values())
        more_entries = len(entries) - max_entries
        if more_entries > 0:
            entries = entries[:max_entries]
            return '<%s %r ... and %s more>' % (klass, entries, more_entries)
        return '<%s %r>' % (klass, entries)


class BoundProxy(object):
    """A proxy bound to a task_exec_history and a decision object.

    This is what gets passed as a dependency in a workflow and has most of the
    scheduling logic that can be reused across different backends.
    The real scheduling is dispatched to the decision object.
    """
    def __init__(self, config, task_exec_history, task_decision, retry=(0,)):
        """Init the bound proxy object.

        Config is used to deserialize results and serialize input arguments.
        The task execution history contains the execution history and is
        used to decide what new tasks should be scheduled.
        The scheduling of new tasks or execution or the execution failure is
        delegated to the task decision object.
        """
        self.config = config
        self.task_exec_history = task_exec_history
        self.task_decision = task_decision
        self.retry = retry
        self.call_number = 0

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
        task_exec_history = self.task_exec_history
        call_number = self.call_number
        self.call_number += 1
        result = Placeholder()
        for retry_number, delay in enumerate(self.retry):
            if task_exec_history.is_timeout(call_number, retry_number):
                continue
            if task_exec_history.is_running(call_number, retry_number):
                break  # result = Placehloder
            if task_exec_history.has_result(call_number, retry_number):
                value = task_exec_history.result(call_number, retry_number)
                order = task_exec_history.order(call_number, retry_number)
                try:
                    value = self.config.deserialize_result(value)
                except Exception as e:
                    self.task_decision.fail(e)
                    break  # result = Placeholder
                result = Result(value, order)
                break
            if task_exec_history.is_error(call_number, retry_number):
                error = task_exec_history.error(call_number, retry_number)
                order = task_exec_history.order(call_number, retry_number)
                result = Error(error, order)
                break
            errors, placeholders, args, kwargs = _scan_args(args, kwargs)
            if errors:
                result = wait_first(errors)
                break
            if placeholders:
                break  # result = Placeholder
            try:
                input_data = self.config.serialize_input(*args, **kwargs)
            except Exception as e:
                self.task_decision.fail(e)
                break  # result = Placeholder
            self.task_decision.schedule(call_number, retry_number, delay,
                                        input_data)
            break  # result = Placeholder
        else:
            # No retries left, it must be a timeout
            order = task_exec_history.order(call_number, retry_number)
            result = Timeout(order)
        return result


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
        * TaskError - if the task failed for whatever reason. This usually means
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
        return self

    def is_error(self):
        """Test if this result represents an error."""
        return False


class Result(TaskResult):
    """The result of a finished task."""
    def __init__(self, result, order):
        self._result = result
        self._order = order

    def result(self):
        return self._result


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


def parallel_reduce(f, iterable):
    """Like reduce() but optimized to maximize paralellel execution.

    The reduce function must be associative and commutative.

    The reduction will start as soon as two results are available, regardless
    of their "position". For example, the following reduction is possible:

    O--------------\
                    O----\
    O--------------/      \
    O--------\             \
              O-----\       O
    O--------/       \     /
    O----\            O---/
          O-----\    /
    O----/       O--/
    O-----------/

    The iterable must have at least one element, otherwise a ValueError will be
    rasied. Note that there is no initializer as the order of the operations
    and of the argumets are not deterministic.

    XXX: The order of the arguments can be preserved and the function
    restrictions relaxed only to associtavitity by keeping track of result
    creation order not only finish order.

    """
    results, non_results = [], []
    for x in iterable:
        if isinstance(x, TaskResult):
            results.append(x)
        else:
            non_results.append(x)
    i = iter(non_results)
    reminder = _sentinel
    for x in i:
        try:
            y = next(i)
            results.append(f(x, y))
        except StopIteration:
            reminder = x
            if not results:  # len(iterable) == 1
                # Wrap the value in a result for uniform interface
                return Result(x, -1, 'xxx')
    if not results:  # len(iterable) == 0
        raise ValueError('parallel_reduce() iterable cannot be empty')
    heapq.heapify(results)
    return _parallel_reduce_recurse(f, results, reminder)


def _parallel_reduce_recurse(f, results, reminder=_sentinel):
    if reminder is not _sentinel:
        new_result = f(reminder, heapq.heappop(results))
        heapq.heappush(results, new_result)
        return _parallel_reduce_recurse(f, results)
    x = heapq.heappop(results)
    try:
        y = heapq.heappop(results)
    except IndexError:
        return x
    new_result = f(x, y)
    heapq.heappush(results, new_result)
    return _parallel_reduce_recurse(f, results)


def _i_or_args(result, results):
    if len(results) == 0:
        return iter(result)
    return (result,) + results


class SuspendTask(BaseException):
    """Special exception raised by result and used for flow control."""


class TaskError(Exception):
    """Raised by result when a task failed its execution."""


class TaskTimedout(TaskError):
    """Raised by result when a task has timedout its execution."""


def _scan_args(args, kwargs):
    new_args, new_kwargs, errs = [], {}, []
    for result in args:
        if isinstance(result, TaskResult):
            try:
                if result.is_error():
                    errs.append(result)
                else:
                    new_args.append(result.result())
            except SuspendTask:
                return [], True, args, kwargs
        else:
            new_args.append(result)
    for key, result in kwargs.items():
        if isinstance(result, TaskResult):
            try:
                if result.is_error():
                    errs.append(result)
                else:
                    new_kwargs[key] = result.result()
            except SuspendTask:
                return [], True, args, kwargs
        else:
            new_kwargs[key] = result
    return errs, False, new_args, new_kwargs


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
