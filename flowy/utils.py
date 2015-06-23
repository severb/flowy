import itertools
import logging
import sys

try:
    import repr as r
except ImportError:
    import reprlib as r


__all__ = ['logger', 'sentinel', 'setup_default_logger', 'i_or_args',
           'short_repr', 'caller_module']


logger = logging.getLogger(__name__.split('.', 1)[0])
sentinel = object()


def setup_default_logger():
    """Configure the default logger for Flowy."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)s\t%(name)s: %(message)s'))
    logger.addHandler(handler)
    logger.setLevel('INFO')
    logger.propagate = False


def i_or_args(result, results):
    """Return an iterable for functions with signature f(arg, *args).
    f can be called with f([a, b, c, ...]) or f(a, b, c, ...).
    In both cases, calling i_or_args(arg, args) returns an iterable
    over a, b, c, ...
    """
    if len(results) == 0:
        return iter(result)
    return (result, ) + results


def str_or_none(val):
    if val is None:
        return None
    return str(val)


class ShortRepr(r.Repr):
    """Make shorter representations on multiple lines."""
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
        for key in itertools.islice(r._possibly_sorted(x), self.maxdict):
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
            pieces = [repr1(elem, newlevel) for elem in itertools.islice(x, maxiter)]
            if n > maxiter: pieces.append('...')
            s = ',\n'.join(pieces)
            if n == 1 and trail: right = trail + right
        return '%s%s%s' % (left, s, right)


short_repr = ShortRepr()


class DescCounter(object):
    """A simple semaphore-like descendent counter."""

    def __init__(self, to=None):
        if to is None:
            self.iterator = itertools.repeat(True)
        else:
            self.iterator = itertools.chain(itertools.repeat(True, to),
                                            itertools.repeat(False))

    def consume(self):
        """Conusme one position; returns True if positions are available."""
        return next(self.iterator)


# Stolen from Pyramid
def caller_module(level=2, sys=sys):
    module_globals = sys._getframe(level).f_globals
    module_name = module_globals.get('__name__') or '__main__'
    module = sys.modules[module_name]
    return module


def caller_package(level=2, caller_module=caller_module):
    # caller_module in arglist for tests
    module = caller_module(level + 1)
    f = getattr(module, '__file__', '')
    if (('__init__.py' in f) or ('__init__$py' in f)):  # empty at >>>
        # Module is a package
        return module  # pragma: no cover
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
