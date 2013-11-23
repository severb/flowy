import logging

from .activity import *
from .config import *
from .workflow import *

_FORMAT = '%(asctime)-15s %(levelname)-10s: %(message)s'
logging.basicConfig(format=_FORMAT, level=logging.INFO)
