def init():

    import functools
    import inspect
    import types


    class MagicBind(object):
        """ Some additional tests for keyword only arguments.

        >>> class Test(object):
        ...     def kw_required(self, *, kw1, kw2, kw3):
        ...         return kw1, kw2, kw3
        ...     def kw_optional(self, *, kw1=1, kw2=2, kw3=3):
        ...         return kw1, kw2, kw3
        ...     def kw(self, *, kw1, kw2=2, kw3=3):
        ...         return kw1, kw2, kw3

        >>> t = Test()
        >>> mb = MagicBind(t, kw2=20)
        >>> mb.kw_required(kw1=1, kw3=3)
        (1, 20, 3)
        >>> mb.kw_required() # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        TypeError:
        >>> mb.kw_optional()
        (1, 20, 3)
        >>> mb.kw(kw1=1)
        (1, 20, 3)
        >>> mb.kw(kw1=1, kw2=2, kw3=3) # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        TypeError:

        """
        def __init__(self, obj, **kwargs):
            self._obj = obj
            self._update_with = kwargs

        def __getattr__(self, name):
            func = getattr(self._obj, name)
            if not callable(func):
                return func
            wrapper = _make_wrapper(func, self._update_with)
            setattr(self, name, wrapper)
            return wrapper

        def __call__(self, *args, **kwargs):
            func = getattr(self._obj, '__call__')
            wrapper = _make_wrapper(func, self._update_with)
            setattr(self, '__call__', wrapper)
            return wrapper(*args, **kwargs)


    def _make_wrapper(func, update_with):
        signature = inspect.signature(func)
        args = {}
        kwargs = {}
        parameters = signature.parameters.values()
        new_parameters = list(parameters)
        for pos, p in enumerate(parameters):
            if p.name not in update_with:
                continue
            if p.kind in [p.VAR_POSITIONAL, p.VAR_KEYWORD]:
                continue
            if p.kind in [p.POSITIONAL_ONLY or p.POSITIONAL_OR_KEYWORD]:
                args[pos] = update_with[p.name]
            if p.kind == p.KEYWORD_ONLY:
                kwargs[p.name] = update_with[p.name]
            del new_parameters[pos + (len(new_parameters) - len(parameters))]

        new_signature = signature.replace(parameters=new_parameters)

        @functools.wraps(func)
        def wrapper(*in_args, **in_kwargs):
            b = new_signature.bind(*in_args, **in_kwargs)
            c_args = list(b.args)
            c_kwargs = dict(b.kwargs)
            for pos, value in args.items():
                c_args[pos:pos] = [value]
            for kwarg, value in kwargs.items():
                if kwarg in c_kwargs:
                    msg = "%s() got multiple values for argument %r"
                    raise TypeError(msg % (func.__name__, kwarg))
                c_kwargs[kwarg] = value

            return func(*c_args, **c_kwargs)

        wrapper.__signature__ = new_signature

        return wrapper

    return MagicBind
