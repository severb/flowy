"""A JSON Encoder similar to TaggedJSONSerializer from Flask, that knows
about Result Proxies and adds some convenience for other common types.

This encoder is a good fit because it will traverse the data structure it
encodes recursively, raising any SuspendTask/TaskError exceptions stored in
task results. Any serializer is supposed to do that.
"""

import json
import uuid
from base64 import b64decode
from base64 import b64encode

from flowy.result import is_result_proxy


__all__ = ['loads', 'dumps']


def dumps(value):
    return json.dumps(_tag(value))

def loads(value):
    return json.loads(value, object_hook=_obj_hook)


def _tag(value):
    if is_result_proxy(value):
        value = value.__wrapped__
    if isinstance(value, tuple):
        return [_tag(x) for x in value]
    elif isinstance(value, uuid.UUID):
        return {' u': value.hex}
    elif isinstance(value, bytes):
        return {' b': b64encode(value).decode('ascii')}
    elif callable(getattr(value, '__json__', None)):
        return _tag(value.__json__())
    elif isinstance(value, list):
        return [_tag(x) for x in value]
    elif isinstance(value, dict):
        return dict((k, _tag(v)) for k, v in value.items())
    return value


def _obj_hook(obj):
    if len(obj) != 1:
        return obj
    key, value = next(iter(obj.items()))
    if key == ' u':
        return uuid.UUID(value)
    elif key == ' b':
        return b64decode(value)
    return obj
