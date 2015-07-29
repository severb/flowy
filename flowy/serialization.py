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


def check_err_and_placeholders(result, value):
    err, placeholders = result
    try:
        wait(value)
    except TaskError:
        if err is None:
            err = value
        else:
            err = first(err, value)
    except SuspendTask:
        placeholders = True
    return err, placeholders


def collect_err_and_results(result, value):
    err, results = result
    if results is None:
        results = []
    if is_result_proxy(value):
        try:
            wait(value)
        except TaskError:
            if err is None:
                err = value
            else:
                err = first(err, value)
        except SuspendTask:
            pass
        else:
            results.append(value)
    return err, results


def traverse_data(value, f=check_err_and_placeholders, initial=(None, False), seen=None):
    """Traverse the data structure.

    >>> traverse_data(1)
    (1, (None, False))
    >>> traverse_data(u'abc') == (u'abc', (None, False))
    True
    >>> traverse_data([1, 2, 3, (4,)])
    ((1, 2, 3, (4,)), (None, False))

    >>> from flowy.result import error, placeholder, result
    >>> r0 = result(u'r0', 0)
    >>> e1 = error('err1', 1)
    >>> e2 = error('err2', 2)
    >>> e3 = error('err3', 3)
    >>> r4 = result(4, 4)
    >>> ph = placeholder()

    Results work just like values
    >>> traverse_data(r0) == (u'r0', (None, False))
    True
    >>> traverse_data([r0, r4]) == ((u'r0', 4), (None, False))
    True
    >>> traverse_data({r0: r4}) == ({u'r0': 4}, (None, False))
    True
    >>> traverse_data((1, 2, 'a', r0)) == ((1, 2, 'a', u'r0'), (None, False))
    True

    Any placeholder should be detected
    >>> r = traverse_data(ph)
    >>> r[0] is ph, r[1]
    (True, (None, True))
    >>> r, (e, p) = traverse_data([r0, [r4, [ph]]])
    >>> r[0] == u'r0' and r[1][0] == 4 and r[1][1][0] is ph, e, p
    (True, None, True)
    >>> r, (e, p) = traverse_data({'a': {r0: {'b': ph}}})
    >>> r['a'][u'r0']['b'] is ph, e, p
    (True, None, True)
    >>> r, (e, p) = traverse_data([[[ph], ph]])
    >>> r[0][0][0] is ph and r[0][1] is ph, e, p
    (True, None, True)

    (Oldest) Error has priority over placeholders flag
    >>> r, (e, p) = traverse_data(e1)
    >>> r is e1, e is e1, p
    (True, True, False)

    >>> r, (e, p) = traverse_data([e3, e1, e2])
    >>> (r[0] is e3 and r[1] is e1 and r[2] is e2), e is e1, p
    (True, True, False)

    >>> r, (e, p) = traverse_data([e3, [e1, [e2]]])
    >>> r[0] is e3 and r[1][0] is e1 and r[1][1][0] is e2, e is e1, p
    (True, True, False)

    >>> r, (e, p) = traverse_data([r0, e2, r4, ph])
    >>> r[0] == u'r0' and r[1] is e2 and r[2] == 4 and r[3] is ph, e is e2, p
    (True, True, True)

    >>> r, (e, p) = traverse_data({r0: {'xyz': e3, (1, 2, r4): {'abc': e1}}})
    >>> r[u'r0']['xyz'] is e3 and r[u'r0'][(1, 2, 4)]['abc'] is e1, e is e1, p
    (True, True, False)

    It should fail on recursive data structures
    >>> a = []
    >>> a.append(a)
    >>> traverse_data(a) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ValueError:  Recursive structure.

    >>> a = {}
    >>> a['x'] = {'y': a}
    >>> traverse_data(a) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ValueError: Recursive structure.

    >>> import itertools
    >>> traverse_data(itertools.count()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ValueError: Unsized iterable too long.

    >>> r, (e, tr) = traverse_data([r4, e1, e2, ph, 'x'], collect_err_and_results, (None, None))
    >>> r[0] == 4 and r[1] is e1 and r[2] is e2 and r[3] is ph and r[4] == 'x', e is e1, tr
    (True, True, [4])

    """
    if seen is None:
        seen = set()

    if is_result_proxy(value):
        try:
            wait(value)
        except TaskError:
            return value, f(initial, value)
        except SuspendTask:
            return value, f(initial, value)

        return value.__wrapped__, f(initial, value)

    if isinstance(value, (bytes, uni)):
        return value, f(initial, value)

    res = initial

    if isinstance(value, collections.Iterable):
        if id(value) in seen:
            raise ValueError('Recursive structure.')
        seen.add(id(value))

    if isinstance(value, collections.Mapping):
        d = {}
        for k, v in value.items():
            k_, res = traverse_data(k, f, res, seen)
            v_, res = traverse_data(v, f, res, seen)
            d[k_] = v_
        return d, res
    if isinstance(value, collections.Iterable):
        max_size = None
        if not isinstance(value, collections.Sized):
            max_size = 2049
        l = []
        for x in value:
            if max_size is not None:
                max_size -= 1
                if max_size == 0:
                    raise ValueError('Unsized iterable too long.')
            x_, res = traverse_data(x, f, res, seen)
            l.append(x_)
        return tuple(l), res
    return value, f(initial, value)


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
