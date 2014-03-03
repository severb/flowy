import functools
import inspect
import logging
import types
from itertools import izip_longest


logging.basicConfig()
logger = logging.root


class MagicBind(object):
    """ Bind specific arguments of object methods for the lazy.

    A quick example of binding a requests session to an URL::

        from requests.sessions import Session
        my_session = Session()
        google_session = MagicBind(my_session, url='http://google.com')
        google_session.get()

    >>> class Test(object):
    ...     a = 100
    ...     def no_args(self):
    ...         return self
    ...     def two_positional(self, x, y):
    ...         return x, y
    ...     def three_positional(self, x, y, z):
    ...         return x, y, z
    ...     def defaults(self, x=1, y=2, z=3):
    ...         return x, y, z
    ...     def args_kwargs(self, x, y, *args, **kwargs):
    ...         return x, y, args, list(sorted(kwargs.items()))
    ...     def only_args_kwargs(self, *args, **kwargs):
    ...         return args, list(sorted(kwargs.items()))
    ...     @staticmethod
    ...     def static(x, y, z):
    ...         return x, y, z
    ...     def __call__(self, x, y, z=3):
    ...         return x, y, z

    >>> t = Test()
    >>> mb = MagicBind(t, x=10)
    >>> mb.a
    100
    >>> mb.no_args() is t
    True
    >>> mb.two_positional(20)
    (10, 20)
    >>> mb.three_positional(20, 30)
    (10, 20, 30)
    >>> mb.defaults(20)
    (10, 20, 3)
    >>> mb.defaults()
    (10, 2, 3)
    >>> mb.defaults(y=20)
    (10, 20, 3)
    >>> mb.args_kwargs(20, 30, 40, z=50, w=60)
    (10, 20, (30, 40), [('w', 60), ('z', 50)])
    >>> mb.only_args_kwargs()
    ((), [])
    >>> mb.args_kwargs(20, 30, x=50, w=60) # doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    TypeError:
    >>> mb.only_args_kwargs(20, 30, 40, z=50, w=60)
    ((20, 30, 40), [('w', 60), ('z', 50)])
    >>> mb.only_args_kwargs(10, 20, x=30, y=40)
    ((10, 20), [('x', 30), ('y', 40)])
    >>> mb.static(20, 30)
    (10, 20, 30)
    >>> t.clble = t
    >>> mb.clble(20)
    (10, 20, 3)

    """
    def __init__(self, obj, **kwargs):
        self._obj = obj
        self._update_with = kwargs

    def __getattr__(self, name):
        func = getattr(self._obj, name)
        if not callable(func):
            return func
        try:
            args, varargs, keywords, defaults = inspect.getargspec(func)
        except TypeError:
            func = getattr(func, '__call__')
            args, varargs, keywords, defaults = inspect.getargspec(func)
        if defaults is None:
            defaults = []
        if isinstance(func, types.MethodType):
            args = args[1:]
        r_avrgs, r_defaults = reversed(args), reversed(defaults)
        sentinel = object()
        new_args, new_defaults = [], []
        for a, d in reversed(list(
            izip_longest(r_avrgs, r_defaults, fillvalue=sentinel)
        )):
            if a not in self._update_with:
                new_args.append(a)
                if d is not sentinel:
                    new_defaults.append(d)
        new_args_count = len(new_args)
        if varargs is not None:
            new_args.append(varargs)
        if keywords is not None:
            new_args.append(keywords)

        # clone the func and change args
        if isinstance(func, types.MethodType):
            code = func.im_func.func_code
        else:
            code = func.func_code
        f_code = types.CodeType(
            new_args_count,
            code.co_nlocals,
            code.co_stacksize,
            code.co_flags,
            code.co_code,
            code.co_consts,
            code.co_names,
            tuple(new_args),
            code.co_filename,
            code.co_name,
            code.co_firstlineno,
            code.co_lnotab
        )
        f_func = types.FunctionType(f_code, {}, None, tuple(new_defaults))

        @functools.wraps(func)
        def wrapper(*positional, **named):
            call_args = inspect.getcallargs(f_func, *positional, **named)
            actual_args = []
            for arg in args:
                actual_args.append(
                    call_args.get(arg, self._update_with.get(arg))
                )
            if varargs is not None:
                actual_args += call_args[varargs]
            actual_kwargs = {}
            if keywords is not None:
                actual_kwargs = call_args[keywords]
            return func(*actual_args, **actual_kwargs)
        setattr(self, name, wrapper)
        return wrapper


def posint_or_none(i):
    if i is not None and int(i) > 0:
        return int(i)


def str_or_none(s):
    if s is not None:
        return str(s)
