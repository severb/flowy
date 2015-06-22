from lazy_object_proxy.slots import Proxy
import collections

from flowy.utils import logger
from flowy.utils import sentinel
from flowy.utils import i_or_args


__all__ = ['result', 'error', 'timeout', 'placeholder', 'copy_result_proxy',
           'wait', 'is_result_proxy', 'SuspendTask', 'TaskError',
           'TaskTimedout', 'restart_type', 'restart']


def result(value, order):
    """A result proxy for a task that has finished successfuly."""
    return ResultProxy(TaskResult(value, order))


def error(reason, order):
    """A result proxy for a task that failed."""
    return ResultProxy(TaskResult(TaskError(reason), order))


def timeout(order):
    """A result proxy for a task that timed out."""
    return ResultProxy(TaskResult(TaskTimedout('A task has timedout'), order))


def placeholder():
    """A result proxy for a task that is either not scheduled or running."""
    return ResultProxy(TaskResult())


def copy_result_proxy(rp):
    assert is_result_proxy(rp)
    factory = rp.__factory__
    return ResultProxy(TaskResult(factory.value, factory.order))


def wait(result):
    """Wait for a task result to complete.

    If the argument is not a task result, this function has no effect.

    This function can raise 3 different types of exceptions:
    * TaskError - if the task failed for whatever reason. This usually means
      the task implementation raised an unhandled exception.
    * TaskTimedout - If the task timed-out on all retry attemps.
    * SuspendTask - This is an internal exception used by Flowy as control
      flow and should not be handled by user code.
    """
    if is_result_proxy(result):
        result.__wrapped__  # force the evaluation


class ResultProxy(Proxy):
    def __repr__(self):
        return repr(self.__wrapped__)


def is_result_proxy(obj):
    """Use this to check if a value is a result proxy without evaluating it."""
    # Use type() instead of isinstance() to avoid the evaluation of the
    # ResultProxy if the object is indeed a proxy.
    return type(obj) is ResultProxy


class TaskResult(object):
    def __init__(self, value=sentinel, order=None):
        self.value = value
        self.order = order
        self.called = False

    def __lt__(self, other):
        if not isinstance(other, TaskResult):
            return NotImplemented
        if self.order is None:
            return False
        if other.order is None:
            return True
        return self.order < other.order

    def __call__(self):
        self.called = True
        if self.is_placeholder():
            raise SuspendTask
        if self.is_error():
            raise self.value
        return self.value

    def is_error(self):
        return isinstance(self.value, Exception)

    def is_placeholder(self):
        return self.value is sentinel

    def __del__(self):
        if not self.called and isinstance(self.value, Exception):
            logger.warning("Result with error was ignored: %s", self.value)


class SuspendTask(BaseException):
    """Special exception raised by result and used for flow control."""


class TaskError(Exception):
    """Raised by result when a task failed its execution."""


class TaskTimedout(TaskError):
    """Raised by result when a task has timedout its execution."""


restart_type = collections.namedtuple('restart_type', 'args kwargs')


def restart(*args, **kwargs):
    """Return an instance of this to restart a workflow with the new input."""
    return restart_type(args, kwargs)
