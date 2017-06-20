from botocore.exceptions import ClientError
import uuid

from flowy.cadence.client import SWFClient
from flowy.cadence.decision import INPUT_SIZE
from flowy.utils import logger
from flowy.proxy import Proxy


def SWFWorkflowStarter(domain, name, version,
                       wid=None, request_id=None,
                       swf_client=None,
                       task_list=None,
                       task_duration=None,
                       workflow_duration=None,
                       serialize_input=None,
                       identity=None):
    """Prepare to start a new workflow, returns a callable.

    The callable should be called only with the input arguments and will
    start the workflow.
    """

    def really_start(*args, **kwargs):
        """Use this function to start a workflow by passing in the args."""
        swf = swf_client if swf_client is not None else SWFClient()
        l_wid = wid  # closure hack
        if l_wid is None:
            l_wid = str(uuid.uuid4())
        l_request_id = request_id
        if l_request_id is None:
            l_request_id = str(uuid.uuid4())
        if serialize_input is None:
            input_data = Proxy.serialize_input(*args, **kwargs)
        else:
            input_data = serialize_input(*args, **kwargs)
        if len(input_data) > INPUT_SIZE:
            logger.error(
                "Input too large: %s/%s" % (len(input_data), INPUT_SIZE))
            raise ValueError('Input too large.')
        try:
            r = swf.start_workflow_execution(
                domain=domain,
                wid=l_wid,
                name=name,
                version=version,
                input=input_data,
                task_list=task_list,
                execution_start_to_close_timeout=task_duration,
                task_start_to_close_timeout=workflow_duration,
                identity=identity,
                request_id=l_request_id)
        except ClientError as e:
            logger.exception('Error while starting the workflow:')
            raise e

        return r.runId

    return really_start
