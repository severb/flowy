import functools
import json
import keyword

import venusian

from flowy.result import is_result_proxy
from flowy.result import restart_type
from flowy.result import TaskError
from flowy.serialization import JSONProxyEncoder
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
            venusian_scanner.registry.register(self, func)

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
        return str(self.name if self.name is not None else func.__name__)

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
        @functools.wraps(factory)
        def wrapper(input_data, *extra_args):
            wf_kwargs = {}
            for dep_name, proxy in self.proxy_factory_registry.items():
                wf_kwargs[dep_name] = proxy(*extra_args)
            func = factory(**wf_kwargs)
            try:
                args, kwargs = self.deserialize_input(input_data)
            except Exception:
                logger.exception('Cannot serialize the result.')
                raise ValueError('Cannot deserialize input.')
            result = func(*args, **kwargs)
            # Can't use directly isinstance(result, restart_type) because if the
            # result is a single result proxy it will be evaluated. This also
            # fixes another issue, on python2 isinstance() swallows any
            # exception while python3 it doesn't.
            if not is_result_proxy(result) and isinstance(result, restart_type):
                r_input_data = self.serialize_restart_input(*result.args, **result.kwargs)
                raise Restart(r_input_data)
            try:
                return self.serialize_result(result)
            except TaskError:
                raise  # let task errors go trough
            except Exception:
                logger.exception('Cannot serialize the result.')
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


class Restart(Exception):
    """Used to notify that an workflow finished with a restart request."""
