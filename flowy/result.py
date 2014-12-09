from flowy.exception import SuspendTask, TaskError, TaskTimedout


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
        raise SuspendTask()


class Error(_Sortable):
    def __init__(self, reason, order):
        self._reason = reason
        self._order = order

    def result(self):
        raise TaskError(self._reason)


class Timeout(_Sortable):
    def __init__(self, reason, order):
        self._reason = reason
        self._order = order

    def result(self):
        raise TaskTimedout(self._reason)


class Result(_Sortable):
    def __init__(self, result, order):
        self._result = result
        self._order = order

    def result(self):
        return self._result
