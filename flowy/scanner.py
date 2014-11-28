import logging
import sys
from functools import partial

import venusian

logger = logging.getLogger(__name__)


def attach(identity, category, task_factory, *a, **kw):
    p_task_factory = partial(task_factory, *a, **kw)

    def callback(venusian_scanner, f_name, obj):
        logger.info("Found %r as '%s' while scanning.",
                    task_factory, f_name)
        f = partial(p_task_factory, f_name=f_name)
        venusian_scanner.registry[identity(f_name, obj)] = f
    venusian.attach(task_factory, callback, category=category)

def scan_for(package=None, ignore=None, categories=None, level=0):
    registry = {}
    scanner = venusian.Scanner(registry=registry)
    if package is None:
        package = caller_package(level=2 + level)
    scanner.scan(package, categories=categories, ignore=ignore)
    return registry


# Stolen from Pyramid

def caller_module(level=2, sys=sys):
    module_globals = sys._getframe(level).f_globals
    module_name = module_globals.get('__name__') or '__main__'
    module = sys.modules[module_name]
    return module


def caller_package(level=2, caller_module=caller_module):
    # caller_module in arglist for tests
    module = caller_module(level+1)
    f = getattr(module, '__file__', '')
    if (('__init__.py' in f) or ('__init__$py' in f)):  # empty at >>>
        # Module is a package
        return module  # pragma: no cover
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
