import uuid

from boto.swf.exceptions import SWFResponseError
from boto.swf.layer1 import Layer1

from flowy import logger, posint_or_none, str_or_none
from flowy.task import serialize_args


class WorkflowStarter(object):
    def __init__(self, domain, name, version,
                 task_list=None,
                 decision_duration=None,
                 workflow_duration=None,
                 client=None):
        self._domain = str(domain)
        self._name = str(name)
        self._version = str(version)
        self._task_list = str_or_none(task_list)
        self._decision_duration = str_or_none(
            posint_or_none(decision_duration)
        )
        self._workflow_duration = str_or_none(
            posint_or_none(workflow_duration)
        )
        self._client = client
        if client is None:  # pragma: no cover
            self._client = Layer1()

    def __call__(self, *args, **kwargs):
        workflow_id = uuid.uuid4()
        return self._start_workflow(workflow_id, *args, **kwargs)

    def with_id(self, workflow_id):
        def wrapper(*args, **kwargs):
            return self._start_workflow(workflow_id, *args, **kwargs)
        return wrapper

    def _start_workflow(self, workflow_id, *args, **kwargs):
        try:
            r = self._client.start_workflow_execution(
                domain=self._domain,
                workflow_id=str(workflow_id),
                workflow_name=self._name,
                workflow_version=self._version,
                task_list=self._task_list,
                input=self._serialize_arguments(*args, **kwargs),
                execution_start_to_close_timeout=self._workflow_duration,
                task_start_to_close_timeout=self._decision_duration
            )
        except SWFResponseError:
            logger.exception('Could not start the workflow:')
            return None
        return r['runId']

    _serialize_arguments = serialize_args
