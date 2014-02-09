import sys

from boto.swf.layer1 import Layer1

from flowy import logger, MagicBind
from flowy.scanner import Scanner
from flowy.spec import ActivitySpecCollector, WorkflowSpecCollector, TaskSpec
from flowy.swf.poller import ActivityPoller, DecisionPoller
from flowy.swf.spec import ActivitySpec as RemoteActivitySpec
from flowy.swf.spec import WorkflowSpec as RemoteWorkflowSpec
from flowy.worker import SingleThreadedWorker


def start_activity_worker(domain, task_list,
                          layer1=None,
                          reg_remote=True,
                          loop=-1,
                          package=None,
                          ignore=None):
    swf_client = _get_client(layer1, domain)
    Spec = TaskSpec
    if reg_remote:
        Spec = RemoteActivitySpec
    scanner = Scanner(ActivitySpecCollector(Spec, swf_client))
    scanner.scan_activities(package=package, ignore=ignore, level=1)
    poller = ActivityPoller(swf_client, task_list)
    worker = SingleThreadedWorker(poller)
    not_registered = scanner.register(worker)
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
    Spec = TaskSpec
    if reg_remote:
        Spec = RemoteWorkflowSpec
    scanner = Scanner(WorkflowSpecCollector(Spec, swf_client))
    scanner.scan_workflows(package=package, ignore=ignore, level=1)
    poller = DecisionPoller(swf_client, task_list)
    worker = SingleThreadedWorker(poller)
    not_registered = scanner.register(worker)
    if not_registered:
        logger.error(
            'Not all workflows could be regestered: %r', not_registered
        )
        sys.exit(1)
    try:
        worker.run_forever(loop)
    except KeyboardInterrupt:
        pass


def _get_client(layer1, domain):
    if layer1 is None:
        layer1 = Layer1()
    return MagicBind(layer1, domain=domain)
