import uuid

from boto.exception import SWFResponseError
from boto.swf.layer1 import Layer1

from flowy.swf.config import cp_encode
from flowy.swf.decision import INPUT_SIZE
from flowy.utils import logger
from flowy.utils import str_or_none
from flowy.proxy import Proxy


def SWFWorkflowStarter(domain, name, version,
                       layer1=None,
                       task_list=None,
                       decision_duration=None,
                       workflow_duration=None,
                       wid=None,
                       tags=None,
                       serialize_input=None,
                       child_policy=None):
    """Prepare to start a new workflow, returns a callable.

    The callable should be called only with the input arguments and will
    start the workflow.
    """
    def really_start(*args, **kwargs):
        """Use this function to start a workflow by passing in the args."""
        l1 = layer1 if layer1 is not None else Layer1()
        l_wid = wid  # closue hack
        if l_wid is None:
            l_wid = uuid.uuid4()
        if serialize_input is None:
            input_data = Proxy.serialize_input(*args, **kwargs)
        else:
            input_data = serialize_input(*args, **kwargs)
        if len(input_data) > INPUT_SIZE:
            logger.error("Input too large: %s/%s" % (len(input_data), INPUT_SIZE))
            raise ValueError('Input too large.')
        try:
            r = l1.start_workflow_execution(
                str(domain), str(l_wid), str(name), str(version),
                task_list=str_or_none(task_list),
                execution_start_to_close_timeout=str_or_none(workflow_duration),
                task_start_to_close_timeout=str_or_none(decision_duration),
                input=str(input_data),
                child_policy=cp_encode(child_policy),
                tag_list=tags_encode(tags))
        except SWFResponseError:
            logger.exception('Error while starting the workflow:')
            raise RuntimeError('Cannot start the workflow.')
        return r['runId']

    return really_start


def tags_encode(tags):
    if tags is None:
        return None
    return list(set(str(t) for t in tags))[:5]
