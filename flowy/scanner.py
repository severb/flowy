import sys

import venusian


def activity(task_id, task_list,
             heartbeat=60,
             schedule_to_close=420,
             schedule_to_start=120,
             start_to_close=300):

    task_list = str(task_list)
    heartbeat = int(heartbeat)
    if heartbeat <= 0:
        raise ValueError('heartbeat must be positive')
    schedule_to_close = int(schedule_to_close)
    if schedule_to_close <= 0:
        raise ValueError('schedule_to_close must be positive')
    schedule_to_start = int(schedule_to_start)
    if schedule_to_start <= 0:
        raise ValueError('schedule_to_start must be positive')
    start_to_close = int(start_to_close)
    if start_to_close <= 0:
        raise ValueError('start_to_close must be positive')

    def wrapper(task_factory):
        def callback(scanner, n, obj):
            scanner.collector.collect(
                task_id=task_id,
                task_factory=task_factory,
                task_list=task_list,
                heartbeat=heartbeat,
                schedule_to_close=schedule_to_close,
                schedule_to_start=schedule_to_start,
                start_to_close=start_to_close,
            )
        venusian.attach(task_factory, callback, category='activity')
        return task_factory
    return wrapper


def workflow(task_id, task_list,
             workflow_duration=3600,
             decision_duration=60):

    task_list = str(task_list)
    workflow_duration = int(workflow_duration)
    if workflow_duration <= 0:
        raise ValueError('workflow_duration must be positive')
    decision_duration = int(decision_duration)
    if decision_duration <= 0:
        raise ValueError('decision_duration must be positive')

    def wrapper(task_factory):
        def callback(scanner, n, obj):
            scanner.collector.collect(
                task_id=task_id,
                task_factory=task_factory,
                task_list=task_list,
                workflow_duration=workflow_duration,
                decision_duration=decision_duration
            )
        venusian.attach(task_factory, callback, category='workflow')
        return task_factory
    return wrapper


class Scanner(object):
    def __init__(self, collector):
        self._collector = collector

    def scan_activities(self, package=None, ignore=None, level=0):
        self._scan(
            categories=('activity',),
            package=package,
            ignore=ignore,
            level=level
        )

    def scan_workflows(self, package=None, ignore=None, level=0):
        self._scan(
            categories=('workflow',),
            package=package,
            ignore=ignore,
            level=level
        )

    def register(self, poller=None):
        self._collector.register(poller)

    def _scan(self, categories=None, package=None, ignore=None, level=0):
        scanner = venusian.Scanner(collector=self._collector)
        if package is None:
            package = caller_package(level=3 + level)
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
        return module  # pragma: no cover
    # Go up one level to get package
    package_name = module.__name__.rsplit('.', 1)[0]
    return sys.modules[package_name]
