import sys
import logging

import venusian
from .spec import SWFActivitySpec, SWFWorkflowSpec


logger = logging.getLogger(__name__)


def swf_activity(version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, name=None):

    def wrapper(activity_factory):
        def callback(scanner, f_name, ob):
            if name is not None:
                f_name = name
            activity_spec = SWFActivitySpec(
                f_name, version, task_list, heartbeat, schedule_to_close,
                schedule_to_start, start_to_close)
            scanner.registry.add(activity_spec, activity_factory)
        venusian.attach(activity_factory, callback, category='activity')
        return activity_factory
    return wrapper


def swf_workflow(version, task_list=None, workflow_duration=None,
                 decision_duration=None, name=None):

    def wrapper(workflow_factory):
        def callback(scanner, f_name, ob):
            if name is not None:
                f_name = name
            workflow_spec = SWFWorkflowSpec(
                f_name, version, task_list, decision_duration,
                workflow_duration)
            scanner.registry.add(workflow_spec, workflow_factory)
        venusian.attach(workflow_factory, callback, category='workflow')
        return workflow_factory
    return wrapper


class TaskRegistry(object):
    def __init__(self):
        self._registry = {}

    def add(self, spec, factory):
        self._registry[spec] = factory

    def __call__(self, spec, *args, **kwargs):
        try:
            fact = self._registry[spec]
        except KeyError:
            logger.warning('Spec %s not found.', spec)
            return lambda: None
        return fact(*args, **kwargs)


class SWFTaskRegistry(TaskRegistry):
    def register_remote(self, swf_client):
        unregistered = []
        # sort it to be deterministic, helps on tests
        for spec in sorted(self._registry.keys()):
            if not spec.register_remote(swf_client):
                unregistered.append(spec)
        return unregistered


class Scanner(object):
    def __init__(self, registry):
        self._registry = registry

    def scan_activities(self, package=None, ignore=None, level=0):
        self._scan(categories=('activity',), package=package, ignore=ignore,
                   level=level)

    def scan_workflows(self, package=None, ignore=None, level=0):
        self._scan(categories=('workflow',), package=package, ignore=ignore,
                   level=level)

    def _scan(self, categories=None, package=None, ignore=None, level=0):
        scanner = venusian.Scanner(registry=self._registry)
        if package is None:
            package = caller_package(level=3 + level)
        scanner.scan(package, categories=categories, ignore=ignore)

    def __call__(self, *args, **kwargs):
        return self._registry(*args, **kwargs)


class SWFScanner(Scanner):
    def __init__(self, registry=None):
        if registry is None:
            registry = SWFTaskRegistry()
        super(SWFScanner, self).__init__(registry)

    def register_remote(self, swf_client):
        return self._registry.register_remote(swf_client)


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
