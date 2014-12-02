from flowy.exception import SuspendTask
from flowy.exception import TaskError
from flowy.exception import TaskTimedout


__all__ ['Placeholder', 'Result', 'Error', 'LinkedError', 'Timeout']


class TaskResult(object):
    _order = None

    def __lt__(self, other):
        if not isinstance(other, TaskResult):
            return NotImplemented
        if self._order is None:
            return False
        if other._order is None:
            return True
        return self._order < other._order


class Placeholder(TaskResult):
    def result(self):
        raise SuspendTask
    wait = result
    is_error = result


class Result(TaskResult):
    def __init__(self, result, d_result, order):
        self._result = result
        self._order = order

    def result(self):
        return self._result

    def wait(self):
        return self

    def is_error(self):
        return False


class Error(Result):
    def __init__(self, reason, order):
        self._reason = reason
        self._order = order

    def result(self):
        raise TaskError(self._reason)

    def is_error(self):
        return True

    def __str__(self):
        return self._reason


class LinkedError(Error):
    def __init__(self, error):
        self._error = error

    def result(self):
        return self._error.result()

    def __lt__(self, other):
        return self._error < other

    def __str__(self):
        return str(self._error)


class Timeout(Error):
    def result(self):
        raise TaskTimedout(self._reason)
