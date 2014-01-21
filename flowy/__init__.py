import logging
from UserDict import DictMixin

# from .activity import *
# from .config import *
# from .workflow import *

_FORMAT = '%(asctime)-15s %(levelname)-10s: %(message)s'
logging.basicConfig(format=_FORMAT, level=logging.INFO)


class NotNoneDict(DictMixin, dict):
    def __init__(self, d=None, **kwargs):
        super(NotNoneDict, self).__init__()
        if d is not None:
            for k, v in d.items():
                if v is not None:
                    self[k] = v
        for k, v in kwargs.items():
            if v is not None:
                self[k] = v

    def __setitem__(self, key, value):
        if value is not None:
            super(NotNoneDict, self).__setitem__(key, value)
