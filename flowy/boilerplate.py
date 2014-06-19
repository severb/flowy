import sys

from boto.swf.layer1 import Layer1
from flowy import logger, MagicBind
from flowy.scanner import SWFScanner
from flowy.poller import SWFWorkflowPoller, SWFActivityPoller
from flowy.worker import SingleThreadedWorker
from flowy.task import AsyncSWFActivity
from flowy.spec import SWFWorkflowSpec
from flowy.starter import SWFWorkflowStarter


def start_activity_worker(domain, task_list,
                          layer1=None,
                          reg_remote=True,
                          loop=-1,
                          package=None,
                          ignore=None):
    swf_client = _get_client(layer1, domain)
    scanner = SWFScanner()
    scanner.scan_activities(package=package, ignore=ignore, level=1)
    poller = SWFActivityPoller(swf_client, task_list, scanner)
    worker = SingleThreadedWorker(poller)
    not_registered = scanner.register(worker)
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


def start_workflow_worker(domain, task_list,
                          layer1=None,
                          reg_remote=True,
                          loop=-1,
                          package=None,
                          ignore=None):
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
                     id=None, tags=None, layer1=None):
    spec = SWFWorkflowSpec(name, version, task_list, decision_duration,
                           workflow_duration)
    client = _get_client(layer1, domain)
    return SWFWorkflowStarter(spec, client, id, tags)


def _get_client(layer1, domain):
    if layer1 is None:
        layer1 = Layer1()
    return MagicBind(layer1, domain=str(domain))
