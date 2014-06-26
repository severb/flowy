import sys
import uuid
import logging
import logging.config

from boto.swf.layer1 import Layer1
from flowy.util import MagicBind
from flowy.poller import SWFActivityPoller, SWFWorkflowPoller
from flowy.proxy import serialize_args
from flowy.scanner import SWFScanner
from flowy.spec import _sentinel, SWFWorkflowSpec
from flowy.task import AsyncSWFActivity
from flowy.worker import SingleThreadedWorker


logger = logging.getLogger(__name__)


def start_activity_worker(domain, task_list, layer1=None, reg_remote=True,
                          loop=-1, package=None, ignore=None, setup_log=True):
    if setup_log:
        _setup_default_logger()
    swf_client = _get_client(layer1, domain)
    scanner = SWFScanner()
    scanner.scan_activities(package=package, ignore=ignore, level=1)
    poller = SWFActivityPoller(swf_client, task_list, scanner)
    worker = SingleThreadedWorker(poller)
    if reg_remote:
        not_registered = scanner.register_remote(swf_client)
        if not_registered:
            logger.error(
                'Not all activities could be regestered: %r', not_registered
            )
            sys.exit(1)
    try:
        worker.run_forever(loop)
    except KeyboardInterrupt:
        pass


def start_workflow_worker(domain, task_list, layer1=None, reg_remote=True,
                          loop=-1, package=None, ignore=None, setup_log=True):
    if setup_log:
        _setup_default_logger()
    swf_client = _get_client(layer1, domain)
    scanner = SWFScanner()
    scanner.scan_workflows(package=package, ignore=ignore, level=1)
    poller = SWFWorkflowPoller(swf_client, task_list, scanner)
    worker = SingleThreadedWorker(poller)
    if reg_remote:
        not_registered = scanner.register_remote(swf_client)
        if not_registered:
            logger.error(
                'Not all workflows could be regestered: %r', not_registered
            )
            sys.exit(1)
    try:
        worker.run_forever(loop)
    except KeyboardInterrupt:
        pass


def async_scheduler(domain, token, layer1=None):
    return AsyncSWFActivity(_get_client(layer1, domain), token)


def workflow_starter(domain, name, version, task_list=None,
                     decision_duration=None, workflow_duration=None,
                     id=None, tags=None, layer1=None, setup_log=True):
    if setup_log:
        _setup_default_logger()
    spec = SWFWorkflowSpec(name, version, task_list, decision_duration,
                           workflow_duration)
    client = _get_client(layer1, domain)
    return SWFWorkflowStarter(spec, client, id, tags)


def _get_client(layer1, domain):
    if layer1 is None:
        layer1 = Layer1()
    return MagicBind(layer1, domain=str(domain))


class SWFWorkflowStarter(object):
    def __init__(self, spec, client, id=None, tags=None):
        self._spec = spec
        self._client = client
        self._id = id
        self._tags = tags

    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, id=_sentinel, tags=_sentinel):
        old_tags = tags
        old_id = id
        if id is not _sentinel:
            self._id = id
        if tags is not _sentinel:
            self._tags = tags

        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            yield
        self._id = old_id
        self._tags = old_tags

    def start(self, *args, **kwargs):
        id = self._id
        if id is None:
            id = uuid.uuid4()
        input = self._serialize_arguments(*args, **kwargs)
        return self._spec.start(self._client, id, input, self._tags)

    _serialize_arguments = serialize_args


def _setup_default_logger():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s %(levelname)s\t%(name)s: %(message)s'
            }},
        'handlers': {
            'console':{
                'class':'logging.StreamHandler',
                'formatter': 'simple'
            }},
        'loggers': {
            'flowy': {
                'handlers': ['console'],
                'popagate': False,
                'level': 'INFO',
            }}
    })
