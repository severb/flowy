import logging



_FORMAT = '%(asctime)-15s %(levelname)-10s: %(message)s'
logging.basicConfig(format=_FORMAT, level=logging.INFO)


from .swf import *
from .activity import *
from .workflow import *
