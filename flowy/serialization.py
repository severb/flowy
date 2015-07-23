"""A JSON Encoder similar to TaggedJSONSerializer from Flask, that knows
about Result Proxies and adds some convenience for other common types.

This encoder is a good fit because it will traverse the data structure it
encodes recursively, raising any SuspendTask/TaskError exceptions stored in
task results. Any serializer is supposed to do that.
"""

import sys
if sys.version_info < (3,):
    uni = unicode
else:
    uni = str

import collections
import json
import uuid
from base64 import b64decode
from base64 import b64encode

from flowy.result import is_result_proxy, TaskError, SuspendTask, wait
from flowy.operations import first


__all__ = ['traverse_data', 'dumps', 'loads']


def traverse_data(value, seen=None):
    """Traveres the data structure and collect errors or placeholders.

    Returns a 3-tuple: traversed data, oldest error, placehoders flag
    The traversed suffers some changes:
        * any mappable becomes a dict
        * any iterator becomes a tuple

    >>> traverse_data(1)
    (1, None, False)
    >>> traverse_data(u'abc') == (u'abc', None, False)
    True
    >>> traverse_data([1, 2, 3, (4,)])
    ((1, 2, 3, (4,)), None, False)

    >>> from flowy.result import error, placeholder, result
    >>> r0 = result(u'r0', 0)
    >>> e1 = error('err1', 1)
    >>> e2 = error('err2', 2)
    >>> e3 = error('err3', 3)
    >>> r4 = result(4, 4)
    >>> ph = placeholder()

    Results work just like values
    >>> traverse_data([r0, r4]) == ((u'r0', 4), None, False)
    True
    >>> traverse_data({r0: r4}) == ({u'r0': 4}, None, False)
    True
    >>> traverse_data((1, 2, 'a', r0)) == ((1, 2, 'a', u'r0'), None, False)
    True

    Any placeholder should be detected
    >>> traverse_data(ph)
    (None, None, True)
    >>> traverse_data([r0, [r4, [ph]]])
    (None, None, True)
    >>> traverse_data({'a': {r0: {'b': ph}}})
    (None, None, True)
    >>> traverse_data([[[ph], ph]])
    (None, None, True)

    (Oldest) Error has priority over placeholders flag
    >>> r = traverse_data(e1)
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    >>> r = traverse_data([e3, e1, e2])
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    >>> r = traverse_data([e3, [e1, [e2]]])
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    >>> r = traverse_data([r0, e2, r4])
    >>> r[0], r[1] is e2, r[2]
    (None, True, False)

    >>> r = traverse_data({r0: {'xyz': e3, (1, 2, r4): {'abc': e1}}})
    >>> r[0], r[1] is e1, r[2]
    (None, True, False)

    It should fail on recursive data structures
    >>> a = []
    >>> a.append(a)
    >>> traverse_data(a) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError

    >>> a = {}
    >>> a['x'] = {'y': a}
    >>> traverse_data(a) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError

    """
    if seen is None:
        seen = set()
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
    if isinstance(value, (bytes, uni)):
        return value, None, False
    if id(value) in seen:
        raise ValueError('Recursive structure.')
    seen.add(id(value))
    if isinstance(value, collections.Mapping):
        d = {}
        for k, v in value.items():
            k_, e, p1 = traverse_data(k, seen)
            if e is not None:
                error = first(error, e) if error is not None else e
            v_, e, p2 = traverse_data(v, seen)
            if e is not None:
                error = first(error, e) if error is not None else e
            placeholders = placeholders or p1 or p2
            d[k_] = v_
        return (d if error is None and not placeholders else None,
                error, placeholders)
    if isinstance(value, collections.Iterable):
        l = []
        for x in value:
            x_, e, p = traverse_data(x, seen)
            if e is not None:
                error = first(error, e) if error is not None else e
            placeholders = placeholders or p
            l.append(x_)
        return (tuple(l) if error is None and not placeholders else None,
                error, placeholders)
    return value, error, placeholders


def dumps(value):
    """Dump a JSON representation of the value, and some extra convenience.

    >>> dumps([1, (2,), {u'x': b'abc'}, u'def'])
    '[1, [2], {"x": {" b": "YWJj"}}, "def"]'

    """
    return json.dumps(_tag(value))


def _tag(value):
    if isinstance(value, uuid.UUID):
        return {' u': value.hex}
    elif isinstance(value, bytes):
        return {' b': b64encode(value).decode('ascii')}
    elif callable(getattr(value, '__json__', None)):
        return _tag(value.__json__())
    elif isinstance(value, (list, tuple)):
        return [_tag(x) for x in value]
    elif isinstance(value, dict):
        return dict((k, _tag(v)) for k, v in value.items())
    return value


def loads(value):
    """Load a JSON value created with dumps:

    >>> (loads(dumps([1, (2,), {u'x': b'abc'}, u'def'])) == 
    ... [1, [2], {u'x': b'abc'}, u'def'])
    True

    """
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
