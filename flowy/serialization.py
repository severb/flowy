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
    """Serialize the data structure and checks for errors or placeholders.

    Returns a 3-tuple: serialized data, oldest error, placehoders flag

    Serializing values should work as expected
    >>> dumps(1)
    ('1', None, False)
    >>> dumps(u'abc')
    ('"abc"', None, False)
    >>> dumps([1, 2, 3, [4]])
    ('[1, 2, 3, [4]]', None, False)

    >>> from flowy.result import error, placeholder, result
    >>> r0 = result(u'r0', 0)
    >>> e1 = error('err1', 1)
    >>> e2 = error('err2', 2)
    >>> e3 = error('err3', 3)
    >>> r4 = result(u'r4', 4)
    >>> ph = placeholder()

    Results work just like values
    >>> dumps([r0, r4])
    ('["r0", "r4"]', None, False)
    >>> dumps({r0: r4})
    ('{"r0": "r4"}', None, False)

    Any placeholder should be detected
    >>> dumps(ph)
    None, None, True
    >>> dumps([r0, [r4, [ph]]])
    None, None, True
    >>> dumps({'a': {r0: {ph: 'b'}}})
    None, None, True
    >>> dumps([[[ph], ph]])
    None, None, True

    (Oldest) Error has priority over placeholders flag
    >>> r = dumps(e1)
    >>> r[0], r[1] is e1, r[2]
    None, True, False

    >>> r = dumps([e3, e1, e2])
    >>> r[0], r[1] is e1, r[2]
    None, True, False

    >>> r = dumps([e3, [e1, [e2]]])
    >>> r[0], r[1] is e1, r[2]
    None, True, False

    >>> r = dumps([r0, e2, r4)
    >>> r[0], r[1] is e2, r[2]
    None, True, False

    >>> r = dumps({r0: {'xyz': {e3: {e2: e1}}}})
    >>> r[0], r[1] is e1, r[2]
    None, True, False

    """
    traversed_value, error, has_placehoders = _traverse(value)
    if error:
        return None, error, False
    if has_placehoders:
        return None, None, True
    else:
        return json.dumps(traversed_value), None, False

def _traverse(value):
    if is_result_proxy(value):
        value = value.__wrapped__
    if isinstance(value, tuple):
        return [_traverse(x) for x in value]
    elif isinstance(value, uuid.UUID):
        return {' u': value.hex}
    elif isinstance(value, bytes):
        return {' b': b64encode(value).decode('ascii')}
    elif callable(getattr(value, '__json__', None)):
        return _traverse(value.__json__())
    elif isinstance(value, list):
        return [_traverse(x) for x in value]
    elif isinstance(value, dict):
        d = {}
        for k, v in value.items():
            if is_result_proxy(k):
                k = k.__wrapped__
            v = _traverse(v)
            d[k] = v
        return d
    return value


def loads(value):
    return json.loads(value, object_hook=_obj_hook)


def _obj_hook(obj):
    if len(obj) != 1:
        return obj
    key, value = next(iter(obj.items()))
    if key == ' u':
        return uuid.UUID(value)
    elif key == ' b':
        return b64decode(value)
    return obj
