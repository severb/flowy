import json
from contextlib import contextmanager

from flowy.util import MagicBind


_sentinel = object()


deserialize_result = json.loads


class TaskProxy(object):

    def __init__(self, retry=[0, 0, 0], error_handling=False,
                 deserialize_result=deserialize_result):
        self._retry = retry
        self._error_handling = error_handling

    deserialize_result = staticmethod(json.loads)

    @staticmethod
    def serialize_arguments(*args, **kwargs):
        r = json.dumps([args, kwargs])
        if len(r) > 32000:
            raise ValueError("Serialized arguments > 32000 characters.")
        return r

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return MagicBind(self, workflow=obj)

    @contextmanager
    def options(self, retry=_sentinel, error_handling=_sentinel):
        old_retry = self._retry
        old_error_handling = self._error_handling
        if retry is not _sentinel:
            self._retry = retry
        if error_handling is not _sentinel:
            self._error_handling = error_handling
        yield
        self._retry = old_retry
        self._error_handling = old_error_handling

    def __call__(self, workflow, *args, **kwargs):
        lookup = workflow._lookup
        if self._error_handling:
            lookup = workflow._lookup_with_errors
        return lookup(self, args, kwargs)

    def __iter__(self):
        return iter(self._retry)
