"""A JSON Encoder similar to TaggedJSONSerializer from Flask, that knows
about Result Proxies and adds some convenience for other common types.

This encoder is a good fit because it will traverse the data structure it
encodes recursively, raising any SuspendTask/TaskError exceptions stored in
task results. Any serializer is supposed to do that.
"""

import collections
import json
import uuid
from base64 import b64decode
from base64 import b64encode

from flowy.result import is_result_proxy, TaskError, SuspendTask, wait
from flowy.operations import first


__all__ = ['traverse_dumps', 'dumps', 'loads']


def traverse_dumps(value):
    """Serialize the data structure and checks for errors or placeholders.

    Returns a 3-tuple: serialized data, oldest error, placehoders flag

    Serializing values should work as expected
    >>> traverse_dumps(1)
    ('1', None, False)
    >>> traverse_dumps(u'abc')
    ('"abc"', None, False)
    >>> traverse_dumps([1, 2, 3, [4]])
    ('[1, 2, 3, [4]]', None, False)

    >>> from flowy.result import error, placeholder, result
    >>> r0 = result(u'r0', 0)
    >>> e1 = error('err1', 1)
    >>> e2 = error('err2', 2)
    >>> e3 = error('err3', 3)
    >>> r4 = result(u'r4', 4)
    >>> ph = placeholder()

    Results work just like values
    >>> traverse_dumps([r0, r4])
    ('["r0", "r4"]', None, False)
    >>> traverse_dumps({r0: r4})
    ('{"r0": "r4"}', None, False)
    >>> traverse_dumps((1, 2, 'a', r0))
    ('[1, 2, "a", "r0"]', None, False)

    Any placeholder should be detected
    >>> traverse_dumps(ph)
    (None, None, True)
    >>> traverse_dumps([r0, [r4, [ph]]])
    (None, None, True)
    >>> traverse_dumps({'a': {r0: {'b': ph}}})
    (None, None, True)
    >>> traverse_dumps([[[ph], ph]])
    (None, None, True)

    (Oldest) Error has priority over placeholders flag
    >>> r = traverse_dumps(e1)
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    >>> r = traverse_dumps([e3, e1, e2])
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    >>> r = traverse_dumps([e3, [e1, [e2]]])
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    >>> r = traverse_dumps([r0, e2, r4])
    >>> r[0], r[1] is e2, r[2]
    (None, True, False)

    >>> r = traverse_dumps({r0: {'xyz': e3, (1, 2, r4): {'abc': e1}}})
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    """
    traversed_value, error, has_placehoders = _traverse(value)
    if error is not None:  # don't deref. the error if it exists
        return None, error, False
    if has_placehoders:
        return None, None, True
    else:
        return dumps(traversed_value), None, False


def _traverse(value):
    try:
        wait(value)
    except TaskError:
        return None, value, False
    except SuspendTask:
        return None, None, True
    placeholders = False
    error = None
    if is_result_proxy(value):
        value = value.__wrapped__
    if isinstance(value, (bytes, unicode)):
        return value, None, False
    if isinstance(value, collections.Mapping):
        d = {}
        for k, v in value.items():
            k_, e, p1 = _traverse(k)
            if e is not None:
                error = first(error, e) if error is not None else e
            v_, e, p2 = _traverse(v)
            if e is not None:
                error = first(error, e) if error is not None else e
            placeholders = placeholders or p1 or p2
            d[k_] = v_
        return d, error, placeholders
    if isinstance(value, collections.Iterable):
        l = []
        for x in value:
            x_, e, p = _traverse(x)
            if e is not None:
                error = first(error, e) if error is not None else e
            placeholders = placeholders or p
            l.append(x_)
        return tuple(l), error, placeholders
    return value, error, placeholders


def dumps(value):
    return json.dumps(_tag(value))


def _tag(value):
    if isinstance(value, uuid.UUID):
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
