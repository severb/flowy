import sys

from boto.swf.layer1 import Layer1
from flowy import logger, MagicBind
from flowy.scanner import SWFScanner
from flowy.poller import SWFWorkflowPoller
from flowy.worker import SingleThreadedWorker
from flowy.task import SWFWorkflow, SWFScheduler


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


# def async_scheduler(domain, layer1=None):
#     return AsyncActivityScheduler(client=_get_client(layer1, domain))


# def workflow_starter(domain, name, version,
#                      task_list=None,
#                      decision_duration=None,
#                      workflow_duration=None,
#                      layer1=None):
#     return WorkflowStarter(
#         name=name,
#         version=version,
#         client=_get_client(layer1, domain),
#         task_list=task_list,
#         decision_duration=decision_duration,
#         workflow_duration=workflow_duration
#     )


def _get_client(layer1, domain):
    if layer1 is None:
        layer1 = Layer1()
    return MagicBind(layer1, domain=str(domain))
