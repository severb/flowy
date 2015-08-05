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
    if not is_result_proxy(value):
        return result
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
        if results is None:
            results = []
        results.append(value)
    return err, results


def traverse_data(value, f=check_err_and_placeholders, initial=(None, False), seen=frozenset(), make_list=True):
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
        seen = seen | frozenset([id(value)])

    if isinstance(value, collections.Mapping):
        d = {}
        for k, v in value.items():
            k_, res = traverse_data(k, f, res, seen, make_list=False)
            v_, res = traverse_data(v, f, res, seen, make_list=make_list)
            d[k_] = v_
        return d, res
    if (
        isinstance(value, collections.Iterable)
        and isinstance(value, collections.Sized)
    ):
        l = []
        for x in value:
            x_, res = traverse_data(x, f, res, seen, make_list=make_list)
            l.append(x_)
        if make_list:
            return l, res
        return tuple(l), res
    if isinstance(value, collections.Iterable):
        raise ValueError('Unsized iterables not allowed.')
    return value, f(initial, value)


def dumps(value):
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
