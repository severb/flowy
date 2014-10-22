def init():

    import functools
    import inspect
    import types
    from itertools import izip_longest


    class MagicBind(object):
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
            if a not in update_with:
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
                    call_args.get(arg, update_with.get(arg))
                )
            if varargs is not None:
                actual_args += call_args[varargs]
            actual_kwargs = {}
            if keywords is not None:
                actual_kwargs = call_args[keywords]
            return func(*actual_args, **actual_kwargs)

        return wrapper

    return MagicBind
