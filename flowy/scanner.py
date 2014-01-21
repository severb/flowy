import sys

import venusian
from flowy import NotNoneDict


def activity_task(task_id, task_list,
                  heartbeat=None,
                  schedule_to_close=None,
                  schedule_to_start=None,
                  start_to_close=None):
    def wrapper(task_factory):
        def callback(scanner, n, obj):
            kwargs = NotNoneDict(
                heartbeat=heartbeat,
                schedule_to_close=schedule_to_close,
                schedule_to_start=schedule_to_start,
                start_to_close=start_to_close
            )
            scanner.collector.collect(
                task_id=task_id,
                task_factory=task_factory,
                task_list=task_list,
                **kwargs
            )
        venusian.attach(task_factory, callback, category='activity')


class Scanner(object):
    def __init__(self, collector):
        self._collector = collector

    def scan_activities(self, package=None, ignore=None):
        self._scan(categories=('activity',), package=package, ignore=ignore)

    def register(self, poller=None):
        self._collector.register(poller)

    def _scan(self, categories=None, package=None, ignore=None):
        scanner = venusian.Scanner(collector=self._collector)
        if package is None:
            package = caller_package(level=3)  # because of the scan call
        scanner.scan(package, categories=categories, ignore=ignore)


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
        return module
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
