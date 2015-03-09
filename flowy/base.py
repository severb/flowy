from __future__ import print_function

import heapq
import logging
import sys
from collections import namedtuple
from functools import partial
from keyword import iskeyword

import venusian
from lazy_object_proxy.slots import Proxy


__all__ = 'restart TaskError TaskTimedout wait first finish_order parallel_reduce'.split()


logger = logging.getLogger(__package__)


_identity = lambda x: x
_sentinel = object()


class Activity(object):
    """A generic activity configuration object."""

    category = None

    def __init__(self, deserialize_input=None, serialize_result=None):
        """Initialize the activity config object.

        The deserialize_input/serialize_result callables are used to
        deserialize the initial input data and serialize the final result.
        By default they are the identity functions.
        """
        # Use default methods for the serialization/deserialization instead of
        # default argument values. This is needed for the local backend and
        # pickle.
        if deserialize_input is not None:
            self.deserialize_input = deserialize_input
        if serialize_result is not None:
            self.serialize_result = serialize_result

    def deserialize_input(self, input_data):
        return input_data

    def serialize_result(self, result):
        return result

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

    def __init__(self, deserialize_input=None, serialize_result=None,
                 serialize_restart_input=None):
        """Initialize the workflow config object.

        The deserialize_input, serialize_result and serialize_restart_input
        callables are used to deserialize the initial input data, serialize the
        final result and serialize the restart arguments.
        """
        super(Workflow, self).__init__(deserialize_input, serialize_result)
        if serialize_restart_input is not None:
            self.serialize_restart_input = serialize_restart_input
        self.proxy_factory_registry = {}

    def serialize_restart_input(self, *args, **kwargs):
        return (args, kwargs)

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
            raise ValueError(
                'Implementation is already registered: %r' % (key,))
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
        except TaskError as e:
            logger.exception('Unhandled task error while initializing the task:')
            decision.fail(e)
        except Exception as e:
            logger.exception('Error while initializing the task:')
            decision.fail(e)
        else:
            try:
                iargs, ikwargs = config.deserialize_input(input_data)
            except Exception as e:
                logger.exception('Error while deserializing the task input:')
                decision.fail(e)
            else:
                try:
                    result = impl(*iargs, **ikwargs)
                except SuspendTask:
                    decision.flush()
                except TaskError as e:
                    logger.exception('Unhandled task error while running the task:')
                    decision.fail(e)
                except Exception as e:
                    logger.exception('Error while running the task:')
                    decision.fail(e)
                else:
                    # Can't use directly isinstance(result, _restart) because
                    # if the result is a single result proxy it will be
                    # evaluated. This also fixes another issue, on python2
                    # isinstance() swallows any exception while python3
                    # doesn't.
                    if not is_result_proxy(result) and isinstance(result, _restart):
                        serialize_restart = getattr(
                            config, 'serialize_restart_input', _identity)
                        try:
                            r_i = serialize_restart(*result.args,
                                                    **result.kwargs)
                        except TaskError as e:
                            logger.exception('Unhandled task error in restart arguments:')
                            decision.fail(e)
                        except Exception as e:
                            logger.exception('Error while serializing restart arguments:')
                            decision.fail(e)
                        except SuspendTask:
                            # There are placeholders in the restart args
                            decision.flush()
                        else:
                            decision.restart(r_i)
                    else:
                        try:
                            result = config.serialize_result(result)
                        except TaskError as e:
                            logger.exception('Unhandled task error in result:')
                            decision.fail(e)
                        except Exception as e:
                            logger.exception('Error while serializing result:')
                            decision.fail(e)
                        except SuspendTask:
                            # There are placeholders in the result
                            decision.flush()
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
            * Finally, if all the arguments look OK, schedule it for execution.
        """
        task_exec_history = self.task_exec_history
        call_number = self.call_number
        self.call_number += 1
        r = placeholder()
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
                    logger.exception('Error while deserializing the activity result:')
                    self.task_decision.fail(e)
                    break  # result = Placeholder
                r = result(value, order)
                break
            if task_exec_history.is_error(call_number, retry_number):
                err = task_exec_history.error(call_number, retry_number)
                order = task_exec_history.order(call_number, retry_number)
                r = error(err, order)
                break
            errors, placeholders = _scan_args(args, kwargs)
            if errors:
                r = first(errors)
                break
            if placeholders:
                break  # result = Placeholder
            try:
                input_data = self.config.serialize_input(*args, **kwargs)
            except Exception as e:
                logger.exception('Error while serializing the task input:')
                self.task_decision.fail(e)
                break  # result = Placeholder
            self.task_decision.schedule(call_number, retry_number, delay,
                                        input_data)
            break  # result = Placeholder
        else:
            # No retries left, it must be a timeout
            order = task_exec_history.order(call_number, retry_number)
            r = timeout(order)
        return r


def result(value, order):
    return ResultProxy(TaskResult(value, order))


def error(reason, order):
    return ResultProxy(TaskResult(TaskError(reason), order))


def timeout(order):
    return ResultProxy(TaskResult(TaskTimedout('A task has timedout'), order))


def placeholder():
    return ResultProxy(TaskResult())


def wait(result):
    """Wait for a task result to complete.

    If the argument is not a task result, this function has no effect.

    This function can raise 3 different types of exceptions:
    * TaskError - if the task failed for whatever reason. This usually means
      the task implementation raised an unhandled exception.
    * TaskTimedout - If the task timed-out on all retry attemps.
    * SuspendTask - This is an internal exception used by Flowy as control
      flow and should not be handled by user code.
    """
    if is_result_proxy(result):
        result.__wrapped__  # force the evaluation


class ResultProxy(Proxy):
    """This is the TaskResult proxy."""
    pass


def is_result_proxy(obj):
    # Use type() instead of isinstance() to avoid the evaluation of the
    # ResultProxy if the object is indeed a proxy.
    return type(obj) is ResultProxy


class TaskResult(object):
    def __init__(self, value=_sentinel, order=None):
        self.value = value
        self.order = order
        self.called = False

    def __lt__(self, other):
        if not isinstance(other, TaskResult):
            return NotImplemented
        if self.order is None:
            return False
        if other.order is None:
            return True
        return self.order < other.order

    def __call__(self):
        self.called = True
        if self.value is _sentinel:  # Placeholder
            raise SuspendTask
        if isinstance(self.value, Exception):
            raise self.value
        return self.value

    def __del__(self):
        if not self.called and isinstance(self.value, Exception):
            logger.warning("Result with error was ignored: %s", self.value)


def _order_key(i):
    return i.__factory__


def first(result, *results):
    """Return the first result finish from a list of results.

    If no one is finished yet - all of the results are placeholders - return
    the first placeholder from the list.
    """
    rs = []
    for r in _i_or_args(result, results):
        if is_result_proxy(r):
            rs.append(r)
        else:
            return r
    return min(rs, key=_order_key)


def finish_order(result, *results):
    """Return the results in their finish order.

    The results that aren't finished yet will be at the end with their relative
    order preserved.
    """
    rs = []
    for r in _i_or_args(result, results):
        if is_result_proxy(r):
            rs.append(r)
        else:
            yield r
    for r in sorted(rs, key=_order_key):
        yield r


def parallel_reduce(f, iterable):
    """Like reduce() but optimized to maximize parallel execution.

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
    raised. Note that there is no initializer as the order of the operations
    and of the arguments are not deterministic.

    The improvement over the built-in reduce() is obtained by starting the
    reduction as soon as any two results are available. The number of reduce
    operations is always constant and equal to len(iterable) - 1 regardless of
    how the reduction graph looks like.

    """
    results, non_results = [], []
    for x in iterable:
        if is_result_proxy(x):
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
                return result(x, -1)
    if not results:  # len(iterable) == 0
        raise ValueError('parallel_reduce() iterable cannot be empty')
    results = [(r.__factory__, r) for r in results]
    heapq.heapify(results)
    return _parallel_reduce_recurse(f, results, reminder)


def _parallel_reduce_recurse(f, results, reminder=_sentinel):
    if reminder is not _sentinel:
        _, first = heapq.heappop(results)
        new_result = f(reminder, first)
        heapq.heappush(results, (new_result.__factory__, new_result))
        return _parallel_reduce_recurse(f, results)
    _, x = heapq.heappop(results)
    try:
        _, y = heapq.heappop(results)
    except IndexError:
        return x
    new_result = f(x, y)
    heapq.heappush(results, (new_result.__factory__, new_result))
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
    errs = []
    for result in args:
        try:
            wait(result)
        except SuspendTask:
            return [], True
        except Exception:
            errs.append(result)
    for key, result in kwargs.items():
        try:
            wait(result)
        except SuspendTask:
            return [], True
        except Exception:
            errs.append(result)
    return errs, False


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
