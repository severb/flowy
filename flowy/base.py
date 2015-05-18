import copy
import functools
import heapq
import json
import logging
import platform
import sys
import tempfile
import warnings
from collections import namedtuple
from functools import partial
from itertools import chain
from itertools import islice
from keyword import iskeyword

import venusian
from lazy_object_proxy.slots import Proxy

try:
    import repr as r
except ImportError:
    import reprlib as r

__all__ = ['restart', 'TaskError', 'TaskTimedout', 'wait', 'first',
           'finish_order', 'parallel_reduce']

logger = logging.getLogger(__name__.split('.', 1)[0])

_identity = lambda x: x
_sentinel = object()


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


class ActivityConfig(object):
    """A simple/generic activity configuration object.

    It only knows about input/result deserialization/serialization and does a
    generic implementation initializaiton.

    It also implements the venusian registration as a syntactic sugar for
    registration.
    """

    category = None  # The category used with venusian
    name = None

    def __init__(self, name=None, deserialize_input=None, serialize_result=None):
        """Initialize the activity config object.

        The deserialize_input/serialize_result callables are used to
        deserialize the initial input data and serialize the final result.

        By default, use a custom JSON Encoder for serialization.

        Any custom serialization should walk the entire data structure, just
        like JSON does, so that any placeholders inside the data structure will
        have a chance to raise SuspendTask and any errors raise TaskError.
        """
        # Use default methods for the serialization/deserialization instead of
        # default argument values. This is needed for the local backend and
        # pickle.
        if deserialize_input is not None:
            self.deserialize_input = deserialize_input
        if serialize_result is not None:
            self.serialize_result = serialize_result
        if name is not None:
            self.name = name

    @staticmethod
    def deserialize_input(input_data):
        """Deserialize the input data in args, kwargs."""
        # raise TypeError if deconstructing fails
        args, kwargs = json.loads(input_data)
        if not isinstance(args, list):
            raise ValueError('Invalid args')
        if not isinstance(kwargs, dict):
            raise ValueError('Invalid kwargs')
        return args, kwargs

    @staticmethod
    def serialize_result(result):
        """Serialize and as a side effect, raise any SuspendTask/TaskErrors."""
        return json.dumps(result, cls=JSONProxyEncoder)

    def __call__(self, func):
        """Associate an activity implementation (callable) to this config.

        The config object can be used as a decorator to bind it to a function
        and make it discoverable later using a scanner (see venusian for more
        details). The decorated function is left untouched.

            @MyConfig(...)
            def x(...):
                ...

            # and later
            some_object.scan()
        """

        def callback(venusian_scanner, *_):
            """This gets called by venusian at scan time."""
            self.register(venusian_scanner.registry, func)

        venusian.attach(func, callback, category=self.category)
        return func

    def wrap(self, func):
        """Wrap the func so that it can be called with serialized input_data.

        The wrapped function can be called with this signature:
        wrapped(input_data, *extra_args)
        This in turn, after deserializing the input_data, will call the original
        func like so: func(*(extra_args + args), **kwargs)

        Finally, the func result is serialized.
        """
        @functools.wraps(func)
        def wrapper(input_data, *extra_args):
            try:
                args, kwargs = self.deserialize_input(input_data)
            except Exception:
                raise ValueError('Cannot deserialize input.')
            result = func(*(tuple(extra_args) + tuple(args)), **kwargs)
            try:
                return self.serialize_result(result)
            except Exception:
                raise ValueError('Cannot serialize the result.')
        return wrapper

    def _get_register_key(self, func):
        return self.name if self.name is not None else func.__name__

    def register(self, registry, func):
        """Register this config and func with the registry.

        Call the registry registration method for this class type and register
        the wrapped func with the config's name. If no name was definded,
        fallback to the func name.
        """
        registry._register(self._get_register_key(func), self.wrap(func))


class WorkflowConfig(ActivityConfig):
    """A simple/generic workflow configuration object with dependencies."""

    def __init__(self, name=None, deserialize_input=None, serialize_result=None,
                 serialize_restart_input=None, proxy_factory_registry=None):
        """Initialize the workflow config object.

        The deserialize_input, serialize_result and serialize_restart_input
        callables are used to deserialize the initial input data, serialize the
        final result and serialize the restart arguments. It uses JSON by
        default.

        See ActivityConfig for a note on serialization.
        """
        super(WorkflowConfig, self).__init__(name, deserialize_input, serialize_result)
        if serialize_restart_input is not None:
            self.serialize_restart_input = serialize_restart_input
        self.proxy_factory_registry = {}
        if proxy_factory_registry is not None:
            self.proxy_factory_registry = dict(proxy_factory_registry)

    def serialize_restart_input(self, *args, **kwargs):
        """Serialize and as a side effect, raise any SuspendTask/TaskErrors."""
        return json.dumps([args, kwargs], cls=JSONProxyEncoder)

    def _check_dep(self, dep_name):
        """Check if dep_name is a unique valid identifier name."""
        # stolen from namedtuple
        if not all(c.isalnum() or c == '_' for c in dep_name):
            raise ValueError(
                'Dependency names can only contain alphanumeric characters and underscores: %r'
                % dep_name)
        if iskeyword(dep_name):
            raise ValueError(
                'Dependency names cannot be a keyword: %r' % dep_name)
        if dep_name[0].isdigit():
            raise ValueError(
                'Dependency names cannot start with a number: %r' % dep_name)
        if dep_name in self.proxy_factory_registry:
            raise ValueError(
                'Dependency name is already registered: %r' % dep_name)

    def conf_proxy(self, dep_name, proxy_factory):
        """Set a proxy factory for a dependency name."""
        self._check_dep(dep_name)
        self.proxy_factory_registry[dep_name] = proxy_factory

    def wrap(self, factory):
        """Wrap the factory so that it can be called with serialized input_data.

        The wrapped factory can be called with this signature:
        wrapped(input_data, *extra_args)
        This will instantiate all proxy factories, passing *extra_args to each
        instance and then, with all proxies, instantiate the factory.
        Finally, the factory instance is called with (*args, **kwargs) and its
        result serialized.

        There are some additional things going on, related to restart handling.
        """
        @functools.wraps(factory)
        def wrapper(input_data, *extra_args):
            wf_kwargs = {}
            for dep_name, proxy in self.proxy_factory_registry.items():
                wf_kwargs[dep_name] = proxy(*extra_args)
            func = factory(**wf_kwargs)
            try:
                args, kwargs = self.deserialize_input(input_data)
            except Exception:
                raise ValueError('Cannot deserialize input.')
            result = func(*args, **kwargs)
            if isinstance(result, _restart):
                restart_input_data = self.serialize_restart_input(*result.args, **result.kwargs)
                raise Restart(restart_input_data)
            try:
                return self.serialize_result(result)
            except Exception:
                raise ValueError('Cannot serialize the result.')
        return wrapper

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
            raise ValueError('Implementation is already registered: %r' %
                             (key, ))
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
            logger.exception(
                'Unhandled task error while initializing the task:')
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
                    logger.exception(
                        'Unhandled task error while running the task:')
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
                    if not is_result_proxy(result) and isinstance(result,
                                                                  _restart):
                        serialize_restart = getattr(config,
                                                    'serialize_restart_input',
                                                    _identity)
                        try:
                            r_i = serialize_restart(*result.args,
                                                    **result.kwargs)
                        except TaskError as e:
                            logger.exception(
                                'Unhandled task error in restart arguments:')
                            decision.fail(e)
                        except Exception as e:
                            logger.exception(
                                'Error while serializing restart arguments:')
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

    def __init__(self, config, task_exec_history, task_decision, retry=(0, )):
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
                    logger.exception(
                        'Error while deserializing the activity result:')
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
                r = copy_result_proxy(first(errors))
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


class TracingBoundProxy(BoundProxy):
    """Similar to a BoundProxy but records task dependency.

    This works by checking every arguments passed to the proxy for task results
    and records the dependency between this call and the previous ones
    generating the task results. It also adds some extra information on the
    task result itself for tracking purposes.

    This can be used with ExecutionTracer to track the execution dependency and
    display in in different forms for analysis.
    """

    def __init__(self, tracer, trace_name, *args, **kwargs):
        super(TracingBoundProxy, self).__init__(*args, **kwargs)
        self.trace_name = trace_name
        self.tracer = tracer

    def __call__(self, *args, **kwargs):
        node_id = "%s-%s" % (self.trace_name, self.call_number)
        r = super(TracingBoundProxy, self).__call__(*args, **kwargs)
        assert is_result_proxy(r)
        factory = r.__factory__
        factory.node_id = node_id
        deps = []
        deps_ids = set()
        for a in args:
            if is_result_proxy(a):
                if id(a) not in deps_ids:
                    deps.append(a)
                    deps_ids.add(id(a))
        for k in kwargs.values():
            if is_result_proxy(k):
                if id(k) not in deps_ids:
                    deps.append(k)
                    deps_ids.add(id(k))
        errors, placeholders = _scan_args(args, kwargs)
        if errors:
            self.tracer.schedule_activity(node_id, self.trace_name)
            self.tracer.flush_scheduled()
            error_factory = first(errors).__factory__
            self.tracer.error(node_id, str(error_factory.value))
        for dep in deps:
            self.tracer.add_dependency(dep.__factory__.node_id, node_id)
        return r


class ShortRepr(r.Repr):
    def __init__(self):
        self.maxlevel = 1
        self.maxtuple = 4
        self.maxlist = 4
        self.maxarray = 4
        self.maxdict = 4
        self.maxset = 4
        self.maxfrozenset = 4
        self.maxdeque = 4
        self.maxstring = self.maxlong = self.maxother = 16

    def repr_dict(self, x, level):
        n = len(x)
        if n == 0: return '{}'
        if level <= 0: return '{...}'
        newlevel = level - 1
        repr1 = self.repr1
        pieces = []
        for key in islice(r._possibly_sorted(x), self.maxdict):
            keyrepr = repr1(key, newlevel)
            valrepr = repr1(x[key], newlevel)
            pieces.append('%s: %s' % (keyrepr, valrepr))
        if n > self.maxdict: pieces.append('...')
        s = ',\n'.join(pieces)
        return '{%s}' % (s, )

    def _repr_iterable(self, x, level, left, right, maxiter, trail=''):
        n = len(x)
        if level <= 0 and n:
            s = '...'
        else:
            newlevel = level - 1
            repr1 = self.repr1
            pieces = [repr1(elem, newlevel) for elem in islice(x, maxiter)]
            if n > maxiter: pieces.append('...')
            s = ',\n'.join(pieces)
            if n == 1 and trail: right = trail + right
        return '%s%s%s' % (left, s, right)


short_repr = ShortRepr()


class ExecutionTracer(object):
    """Record the execution history for display and analysis."""

    def __init__(self):
        self.reset()

    def schedule_activity(self, node_id, name):
        assert node_id not in self.nodes
        self.nodes[node_id] = name
        self.current_schedule.append(node_id)
        self.timeouts[node_id] = 0
        self.activities.add(node_id)

    def schedule_workflow(self, node_id, name):
        assert node_id not in self.nodes
        self.nodes[node_id] = name
        self.current_schedule.append(node_id)
        self.timeouts[node_id] = 0

    def flush_scheduled(self):
        self.levels.append(self.current_schedule)
        self.current_schedule = []

    def result(self, node_id, result):
        assert node_id in self.nodes
        assert node_id not in self.levels
        self.levels.append(node_id)
        self.results[node_id] = result

    def error(self, node_id, reason):
        assert node_id in self.nodes
        assert node_id not in self.levels
        self.levels.append(node_id)
        self.errors[node_id] = reason

    def timeout(self, node_id):
        assert node_id in self.nodes
        assert node_id not in self.results or node_id not in self.errors
        self.timeouts[node_id] += 1

    def add_dependency(self, from_node, to_node):
        """ node_id -> node_id """
        self.deps.setdefault(from_node, []).append(to_node)

    def copy(self):
        et = ExecutionTracer()
        et.__dict__ = copy.deepcopy(self.__dict__)
        return et

    def reset(self):
        self.levels = []
        self.current_schedule = []
        self.timeouts = {}
        self.results = {}
        self.errors = {}
        self.activities = set()
        self.deps = {}
        self.nodes = {}

    def to_dot(self):
        try:
            import pygraphviz as pgv
        except ImportError:
            warnings.warn('Extra requirements for "trace" are not available.')
            return
        graph = pgv.AGraph(directed=True, strict=False)

        hanging = set()
        for node_id, node_name in self.nodes.items():
            shape = 'box'
            if node_id in self.activities:
                shape = 'ellipse'
            finish_id = 'finish-%s' % node_id
            color, fontcolor = 'black', 'black'
            if node_id in self.errors:
                color, fontcolor = 'red', 'red'
            graph.add_node(node_id,
                           label=node_name,
                           shape=shape,
                           width=0.8,
                           color=color,
                           fontcolor=fontcolor)
            if node_id in self.results or node_id in self.errors:
                if node_id in self.errors:
                    rlabel = str(self.errors[node_id])
                else:
                    rlabel = short_repr.repr(self.results[node_id])
                    rlabel = ' ' + '\l '.join(rlabel.split('\n'))  # Left align
                graph.add_node(finish_id,
                               label='',
                               shape='point',
                               width=0.1,
                               color=color)
                graph.add_edge(node_id, finish_id,
                               arrowhead='none',
                               penwidth=3,
                               fontsize=8,
                               color=color,
                               fontcolor=fontcolor,
                               label='  ' + rlabel)
            else:
                hanging.add(node_id)

        levels = ['l%s' % i for i in range(len(self.levels))]
        for l in levels:
            graph.add_node(l,
                           shape='point',
                           label='',
                           width=0.1,
                           style='invis')
        if levels:
            start = levels[0]
            for l in levels[1:]:
                graph.add_edge(start, l, style='invis')
                start = l

        for l_id, l in zip(levels, self.levels):
            if isinstance(l, list):
                graph.add_subgraph([l_id] + l, rank='same')
            else:
                graph.add_subgraph([l_id, 'finish-%s' % l], rank='same')

        for from_node, to_nodes in self.deps.items():
            if from_node in hanging:
                hanging.remove(from_node)
            color = 'black'
            style = ''
            if from_node in self.errors:
                color = 'red'
                from_node = 'finish-%s' % from_node
            elif from_node in self.results:
                from_node = 'finish-%s' % from_node
            else:
                style = 'dotted'
            for to_node in to_nodes:
                graph.add_edge(from_node, to_node, color=color, style=style)

        if hanging:
            for node_id in hanging:
                finish_id = 'finish-%s' % node_id
                graph.add_node(finish_id,
                               label='',
                               shape='point',
                               width=0.1,
                               style='invis')
                graph.add_edge(node_id, finish_id,
                               style='dotted',
                               arrowhead='none')
            # l_id is the last level here
            graph.add_subgraph([l_id] + ['finish-%s' % h for h in hanging],
                               rank='same')

        for node_id in self.nodes:
            retries = self.timeouts[node_id]
            if retries:
                graph.add_edge(node_id, node_id,
                               label=' %s' % retries,
                               color='orange',
                               fontcolor='orange',
                               fontsize=8)

        return graph

    def display(self):
        graph = self.to_dot()
        if not graph:
            return
        tf = tempfile.NamedTemporaryFile(mode='w+b',
                                         prefix='dot_',
                                         suffix='.svg',
                                         delete=False)
        graph.draw(tf.name, format='svg', prog='dot')
        logger.info('Workflow execution traced: %s', tf.name)
        import webbrowser
        webbrowser.open(tf.name)


def result(value, order):
    return ResultProxy(TaskResult(value, order))


def error(reason, order):
    return ResultProxy(TaskResult(TaskError(reason), order))


def timeout(order):
    return ResultProxy(TaskResult(TaskTimedout('A task has timedout'), order))


def placeholder():
    return ResultProxy(TaskResult())


def copy_result_proxy(rp):
    assert is_result_proxy(rp)
    factory = rp.__factory__
    return ResultProxy(TaskResult(factory.value, factory.order))


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
    def __repr__(self):
        return repr(self.__wrapped__)


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
        if self.is_placeholder():
            raise SuspendTask
        if self.is_error():
            raise self.value
        return self.value

    def is_error(self):
        return isinstance(self.value, Exception)

    def is_placeholder(self):
        return self.value is _sentinel

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


def parallel_reduce(f, iterable, initializer=_sentinel):
    """Like reduce() but optimized to maximize parallel execution.

    The reduce function must be associative and commutative.

    The reduction will start as soon as two results are available, regardless
    of their "position". For example, the following reduction is possible:

     5 ----1-----|
    15           --------------4----------|
    15           |                        -------------12|
    15           |                        |              -------------17|
  R 15           |                        |              |              -------------21
    15 ----------|---2-----|              |              |              |
    15           |         --------------8|              |              |
    10 ---------3|         |                             |              |
    60 --------------------|-----------------------------|--------4-----|
    50 --------------------|----------------------------5|
    20 -------------------6|

    The iterable must have at least one element, otherwise a ValueError will be
    raised. Note that there is no initializer as the order of the operations
    and of the arguments are not deterministic.

    The improvement over the built-in reduce() is obtained by starting the
    reduction as soon as any two results are available. The number of reduce
    operations is always constant and equal to len(iterable) - 1 regardless of
    how the reduction graph looks like.

    """
    if initializer is not _sentinel:
        iterable = chain([initializer], iterable)
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
        raise ValueError(
            'parallel_reduce() of empty sequence with no initial value')
    if is_result_proxy(results[0]):
        results = [(r.__factory__, r) for r in results]
        heapq.heapify(results)
        return _parallel_reduce_recurse(f, results, reminder)
    else:
        # Looks like we don't use a task for reduction, fallback on reduce
        return reduce(f, results)


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
    return (result, ) + results


class SuspendTask(BaseException):
    """Special exception raised by result and used for flow control."""


class TaskError(Exception):
    """Raised by result when a task failed its execution."""


class TaskTimedout(TaskError):
    """Raised by result when a task has timedout its execution."""


def _scan_args(args, kwargs):
    errs = []
    placeholders = False
    for result in args:
        try:
            wait(result)
        except SuspendTask:
            placeholders = True
        except Exception:
            errs.append(result)
    for key, result in kwargs.items():
        try:
            wait(result)
        except SuspendTask:
            placeholders = True
        except Exception:
            errs.append(result)
    return errs, placeholders


_restart = namedtuple('restart', 'args kwargs')


def restart(*args, **kwargs):
    """Return an instance of this to restart a workflow with the new input."""
    return _restart(args, kwargs)

class Restart(Exception):
    """Used to signal a restart request and hold the serialized arguments."""


def setup_default_logger():
    """Configure the default logger for Flowy."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)s\t%(name)s: %(message)s'))
    logger.addHandler(handler)
    logger.setLevel('INFO')
    logger.propagate = False


# Stolen from Pyramid
def _caller_module(level=2, sys=sys):
    module_globals = sys._getframe(level).f_globals
    module_name = module_globals.get('__name__') or '__main__'
    module = sys.modules[module_name]
    return module


def _caller_package(level=2, caller_module=_caller_module):
    # caller_module in arglist for tests
    module = caller_module(level + 1)
    f = getattr(module, '__file__', '')
    if (('__init__.py' in f) or ('__init__$py' in f)):  # empty at >>>
        # Module is a package
        return module  # pragma: no cover
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
