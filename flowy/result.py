from flowy.exception import SuspendTask
from flowy.exception import TaskError
from flowy.exception import TaskTimedout


class _Sortable(object):
    _order = None

    def __lt__(self, other):
        if not isinstance(other, _Sortable):
            return NotImplemented
        if self._order is None:
            return False
        if other._order is None:
            return True
        return self._order < other._order


class Placeholder(_Sortable):
    def result(self):
        raise SuspendTask


class Result(_Sortable):
    def __init__(self, result, d_result, order):
        self._result = result
        self._d_result = d_result
        self._order = order

    def result(self):
        if not hasattr(self, '_result_cache'):
            try:
                self._result_cache = self._d_result(self._result)
            except Exception as e:
                self._result_cache = TaskError(e)
        if isinstance(self._result_cache, Exception):
            raise self._result_cache
        return self._result_cache


class Error(Result):
    def __init__(self, reason, order):
        self._reason = reason
        self._order = order

    def result(self):
        raise TaskError(self._reason)


class LinkedError(Error):
    def __init__(self, error):
        self._error = error

    def result(self):
        return self._error.result()

    def __lt__(self, other):
        return self._error < other


class Timeout(Error):
    def __init__(self, reason, order):
        self._reason = reason
        self._order = order

    def result(self):
        raise TaskTimedout(self._reason)
