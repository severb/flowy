import json
from contextlib import contextmanager

from flowy.util import MagicBind


_sentinel = object()


class TaskProxy(object):

    from flowy.result import Placeholder
    from flowy.result import Result
    from flowy.result import Error
    from flowy.result import LinkedError
    from flowy.result import Timeout

    def __init__(self, retry=[0, 0, 0]):
        self._retry = retry

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return MagicBind(self, workflow=obj)

    @contextmanager
    def options(self, retry=_sentinel):
        old_retry = self._retry
        if retry is not _sentinel:
            self._retry = retry
        yield
        self._retry = old_retry

    def __call__(self, workflow, *args, **kwargs):
        return wolkflow._lookup(self, args, kwargs)

    def __iter__(self):
        return iter(self._retry)
