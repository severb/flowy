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
    >>> mb.args_kwargs(2, 3, x=5, w=6) # doctest:+IGNORE_EXCEPTION_DETAIL
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
    >>> mb(20, 30)
    (10, 20, 30)

"""
import sys


_PY2 = sys.version_info[0] == 2
_PY3 = sys.version_info[0] == 3


if _PY2:
    from flowy.util2 import init
if _PY3:
    from flowy.util3 import init

MagicBind = init()
MagicBind.__module__ = __name__
