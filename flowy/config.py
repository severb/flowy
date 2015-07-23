import functools
import json
import keyword

import venusian

from flowy.result import is_result_proxy
from flowy.result import restart_type
from flowy.result import SuspendTask
from flowy.result import TaskError
from flowy.result import wait
from flowy.serialization import dumps
from flowy.serialization import loads
from flowy.serialization import traverse_data
from flowy.utils import logger


__all__ = ['ActivityConfig', 'WorkflowConfig', 'Restart']


class ActivityConfig(object):
    """A simple/generic activity configuration object.

    It only knows about input/result deserialization/serialization and does a
    generic implementation initializaiton.

    It also implements the venusian registration as a syntactic sugar for
    registration.
    """

    category = None  # The category used with venusian

    def __init__(self, deserialize_input=None, serialize_result=None):
        """Initialize the activity config object.

        The deserialize_input/serialize_result callables are used to
        deserialize the initial input data and serialize the final result.

        By default, use a custom JSON Encoder for serialization.

        Custom serializers must walk the entire data structure. This ensures
        that any placeholder or error objects in the data structure will have a
        chance to raise.
        """
        # Use default methods for the serialization/deserialization instead of
        # default argument values. This is convenient for the local backend
        # that uses pickle.
        if deserialize_input is not None:
            self.deserialize_input = deserialize_input
        if serialize_result is not None:
            self.serialize_result = serialize_result

    @staticmethod
    def deserialize_input(input_data):
        """Deserialize the input data in args, kwargs."""
        args, kwargs = loads(input_data)  # raise TypeError if deconstructing fails
        if not isinstance(args, list):
            raise ValueError('Invalid args: %r' % (args,))
        if not isinstance(kwargs, dict):
            raise ValueError('Invalid kwargs: %r' % (kwargs,))
        return args, kwargs

    @staticmethod
    def serialize_result(result):
        """Serialize the result."""
        return dumps(result)

    def __call__(self, key=None):
        """Associate an activity implementation (callable) to this config.

        The config object can be used as a decorator to bind it to a function
        and make it discoverable later using a scanner (see venusian for more
        details). The decorated function is left untouched.

            my_config = ActivityConfig(...)

            @my_config(key='my_name')
            def x(...):
                ...

            # and later
            worker.scan()
        """
        def conf_deco(func):

            def callback(venusian_scanner, *_):
                """This gets called by venusian at scan time."""
                self.register(venusian_scanner, key, func)

            venusian.attach(func, callback, category=self.category)
            return func

        return conf_deco

    def register(self, registry, key, func):
        if key is None:
            key = func.__name__
        registry.register_task(key, self.wrap(func))

    def wrap(self, func):
        """Wrap the func so that it can be called with serialized input_data.

        The wrapped function can be called with this signature:
        wrapped(input_data, *extra_args)
        This in turn, after deserializing the input_data, will call the original
        func like so: func(*(extra_args + args), **kwargs)

        Finally, the func result is serialized.
        """
        # We can't pickle closures, thus making multiprocessing ->
        # concurrent.futures -> local backend fail. Instead, use partials.
        # This won't work on python 2.6, https://bugs.python.org/issue5228
        return functools.partial(_activity_wrapper, self, func)


def _activity_wrapper(self, func, input_data, *extra_args):
    try:
        args, kwargs = self.deserialize_input(input_data)
    except Exception:
        logger.exception('Cannot deserialize the input:')
        raise ValueError('Cannot deserialize the input: %r' % (input_data,))
    result = func(*(tuple(extra_args) + tuple(args)), **kwargs)
    try:
        return self.serialize_result(result)
    except Exception:
        logger.exception('Cannot serialize the result:')
        raise ValueError('Cannot serialize the result: %r' % (result,))


class WorkflowConfig(ActivityConfig):
    """A simple/generic workflow configuration object with dependencies."""

    def __init__(self, deserialize_input=None, serialize_result=None,
                 serialize_restart_input=None):
        """Initialize the workflow config object.

        The deserialize_input, serialize_result and serialize_restart_input
        callables are used to deserialize the initial input data, serialize the
        final result and serialize the restart arguments. It uses JSON by
        default.

        See ActivityConfig for a note on serialization.
        """
        super(WorkflowConfig, self).__init__(deserialize_input, serialize_result)
        if serialize_restart_input is not None:
            self.serialize_restart_input = serialize_restart_input
        self.proxy_factory_registry = {}

    def serialize_restart_input(self, *args, **kwargs):
        """Try to serialize the result, returns any errors or placeholders."""
        return dumps([args, kwargs])

    @staticmethod
    def serialize_result(result):
        """Try to serialize the result, returns any errors or placeholders."""
        return dumps(result)

    def _check_dep(self, dep_name):
        """Check if dep_name is a unique valid identifier name."""
        # stolen from namedtuple
        if not all(c.isalnum() or c == '_' for c in dep_name):
            raise ValueError(
                'Dependency names can only contain alphanumeric characters and underscores: %r'
                % dep_name)
        if keyword.iskeyword(dep_name):
            raise ValueError(
                'Dependency names cannot be a keyword: %r' % dep_name)
        if dep_name[0].isdigit():
            raise ValueError(
                'Dependency names cannot start with a number: %r' % dep_name)
        if dep_name in self.proxy_factory_registry:
            raise ValueError(
                'Dependency name is already registered: %r' % dep_name)

    def conf_proxy_factory(self, dep_name, proxy_factory):
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
        # See Activity.wrap for an explanation why there isn't a closure here.
        return functools.partial(_workflow_wrapper, self, factory)

    def __repr__(self):
        klass = self.__class__.__name__
        deps = sorted(self.proxy_factory_registry.keys())
        max_entries = 5
        more_deps = len(deps) - max_entries
        if more_deps > 0:
            deps = deps[:max_entries] + ['... and %s more' % more_deps]
        return '<%s deps=%s>' % (klass, ','.join(deps))


def _workflow_wrapper(self, factory, input_data, *extra_args):
    wf_kwargs = {}
    for dep_name, proxy in self.proxy_factory_registry.items():
        wf_kwargs[dep_name] = proxy(*extra_args)
    func = factory(**wf_kwargs)
    try:
        args, kwargs = self.deserialize_input(input_data)
    except Exception:
        logger.exception('Cannot deserialize the input:')
        raise ValueError('Cannot deserialize the input: %r' % (input_data,))
    result = func(*args, **kwargs)
    # Can't use directly isinstance(result, restart_type) because if the
    # result is a single result proxy it will be evaluated. This also
    # fixes another issue, on python2 isinstance() swallows any
    # exception while python3 it doesn't.
    if not is_result_proxy(result) and isinstance(result, restart_type):
        try:
            traversed_input, (error, placeholders) =  traverse_data(
                [result.args, result.kwargs])
        except Exception:
            logger.exception('Cannot traverse the restart arguments:')
            raise ValueError(
                'Cannot traverse the restart arguments: %r, %r' %
                result.args, result.kwargs)
        wait(error)  # raise if not None
        if placeholders:
            raise SuspendTask
        r_args, r_kwargs = traversed_input
        try:
            serialized_input = self.serialize_restart_input(*r_args, **r_kwargs)
        except Exception:
            logger.exception('Cannot serialize the restart arguments:')
            raise ValueError(
                'Cannot serialize the restart arguments: %r, %r' %
                result.args, result.kwargs)
        raise Restart(serialized_input)
    try:
        traversed_result, (error, placeholders) = traverse_data(result)
    except Exception:
        logger.exception('Cannot traverse the result:')
        raise ValueError('Cannot traverse the result: %r' % result)
    wait(error)
    if placeholders:
        raise SuspendTask
    try:
        return self.serialize_result(traversed_result)
    except Exception:
        logger.exception('Cannot serialize the result:')
        raise ValueError('Cannot serialize the result: %r' % (result,))


class Restart(Exception):
    """Used to notify that an workflow finished with a restart request."""
    def __init__(self, input_data):
        self.input_data = input_data
