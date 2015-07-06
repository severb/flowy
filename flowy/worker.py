import venusian

from flowy.config import Restart
from flowy.result import SuspendTask
from flowy.result import TaskError
from flowy.utils import logger
from flowy.utils import caller_package


__all__ = ['Worker']


class Worker(object):
    """A runner for all registered wrapped functions."""

    categories = []  # venusian categories to scan for

    def __init__(self):
        self.registry = {}

    def register(self, config, func, key=None):
        """Register a config and a function with a key."""
        config.register(self, key, func)

    def register_task(self, key, wrapped_func):
        """Register a wrapped task and its key.

        This can executed later by calling Worker instances and passing them
        the same key used for registration.
        """
        if key in self.registry:
            raise ValueError('Implementation is already registered: %r' % (key, ))
        self.registry[key] = wrapped_func

    def __call__(self, key, input_data, decision, *extra_args):
        """Execute the wrapped func registered with key passing the input_data.

        Any exra_args are also passed along to the wrapped_key.

        The actual actions are dispatched to the decision object and can be one
        of:
            * flush() - nothing to do, any pending actions should be commited
            * fail(e) - ignore pending actions, fail the execution
            * finish(e) - ignore pending actions, complete the execution
            * restart(serialized_input) - ignore pending actions, restart the execution
        """
        try:
            wrapped_func = self.registry[key]
        except KeyError:
            logger.error("Colud not find implementation for key: %r", (key,))
            return  # Let it timeout
        try:
            serialized_result = wrapped_func(input_data, *extra_args)
        except SuspendTask:  # only from workflows
            decision.flush()
        except TaskError as e:  # only from workflows
            logger.exception('Unhandled task error in task:')
            decision.fail(e)
        except Restart as e:  # only from workflows
            decision.restart(e.input_data)
        except Exception as e:
            logger.exception('Unhandled exception in task:')
            decision.fail(e)
        else:
            decision.finish(serialized_result)

    def scan(self, categories=None, package=None, ignore=None, level=0):
        """Scan for registered implementations and their configs.

        The categories can be used to scan for only a subset of tasks. By
        default it will use the categories property set on the class.

        Use venusian to scan. By default it will scan the package of the caller
        of the scan method but this can be changed using the package and ignore
        arguments. Their semantics is the same with the ones in venusian
        documentation.

        The level represents the additional stack frames to add to the caller
        package identification code. This is useful when this call happens
        inside another function.
        """
        if categories is None:
            categories = self.categories
        scanner = self.make_scanner()
        if package is None:
            package = caller_package(level=2 + level)
        scanner.scan(package, categories=categories, ignore=ignore)

    def make_scanner(self):
        return venusian.Scanner(register_task=self.register_task)

    def __repr__(self):
        klass = self.__class__.__name__
        max_entries = 5
        entries = sorted(self.registry.values())
        more_entries = len(entries) - max_entries
        if more_entries > 0:
            entries = entries[:max_entries]
            return '<%s %r ... and %s more>' % (klass, entries, more_entries)
        return '<%s %r>' % (klass, entries)
