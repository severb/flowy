"""A JSON Encoder that knows about Result Proxies.

This encoder is a good fit because it will traverse the data strucutre it
encodes reucrsively raising any SuspendTask/TaskError exceptions stored in the
result proxies. Any encoder is supposed to do that.
"""

import json
import sys
import platform

from flowy.result import is_result_proxy


__all__ = ['JSONProxyEncoder']


class JSONProxyEncoder(json.JSONEncoder):
    # Patch the hell out of it!
    # The pure Python implementation uses isinstance() which works on proxy
    # objects but the C implementation uses a stricter check that won't work.
    def encode(self, o):
        if is_result_proxy(o):
            o = o.__wrapped__
        return super(JSONProxyEncoder, self).encode(o)

    def default(self, obj):
        if is_result_proxy(obj):
            return obj.__wrapped__
        return json.JSONEncoder.default(self, obj)

    # On py26 things are a bit worse...
    if sys.version_info[:2] == (2, 6):

        def _iterencode(self, o, markers=None):
            s = super(JSONProxyEncoder, self)
            if is_result_proxy(o):
                return s._iterencode(o.__wrapped__, markers)
            return s._iterencode(o, markers)

    # pypy uses simplejson, and ...
    if platform.python_implementation() == 'PyPy':

        def _JSONEncoder__encode(self, o, markers, builder,
                                 _current_indent_level):
            s = super(JSONProxyEncoder, self)
            if is_result_proxy(o):
                o = o.__wrapped__
            return s._JSONEncoder__encode(o, markers, builder,
                                          _current_indent_level)
