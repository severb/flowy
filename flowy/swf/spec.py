from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError

from flowy import logger
from flowy.spec import RemoteTaskSpec


class ActivitySpec(RemoteTaskSpec):
    def __init__(self, task_id, task_factory, client, task_list, heartbeat,
                 schedule_to_close, schedule_to_start, start_to_close):
        self._name = task_id.name
        self._version = task_id.version
        super(ActivitySpec, self).__init__(
            task_id=task_id,
            task_factory=task_factory,
            client=client
        )
        self._task_list = task_list
        # Amazon SWF expects these values to be strings
        self._heartbeat = str(heartbeat)
        self._schedule_to_close = str(schedule_to_close)
        self._schedule_to_start = str(schedule_to_start)
        self._start_to_close = str(start_to_close)

    def _try_register_remote(self):
        try:
            self._client.register_activity_type(
                name=self._name,
                version=self._version,
                task_list=self._task_list,
                default_task_heartbeat_timeout=self._heartbeat,
                default_task_schedule_to_close_timeout=self._schedule_to_close,
                default_task_schedule_to_start_timeout=self._schedule_to_start,
                default_task_start_to_close_timeout=self._start_to_close,
            )
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError:
            logger.exception('Error while registering activity:')
            return False
        return True

    def _check_if_compatible(self):
        try:
            c = self._client.describe_activity_type(
                activity_name=self._name,
                activity_version=self._version
            )['configuration']
        except SWFResponseError:
            logger.exception('Error while checking activity compatibility:')
            return False
        return all([
            c['defaultTaskList']['name'] == self._task_list,
            c['defaultTaskHeartbeatTimeout'] == self._heartbeat,
            c['defaultTaskScheduleToCloseTimeout'] == self._schedule_to_close,
            c['defaultTaskScheduleToStartTimeout'] == self._schedule_to_start,
            c['defaultTaskStartToCloseTimeout'] == self._start_to_close
        ])


class WorkflowSpec(RemoteTaskSpec):
    def __init__(self, task_id, task_factory, client, task_list,
                 decision_duration, workflow_duration):
        self._name = task_id.name
        self._version = task_id.version
        super(WorkflowSpec, self).__init__(
            task_id=task_id,
            task_factory=task_factory,
            client=client
        )
        # Amazon SWF expects these values to be strings
        self._task_list = task_list
        self._decision_duration = str(decision_duration)
        self._workflow_duration = str(workflow_duration)

    def _try_register_remote(self):
        workflow_duration = self._workflow_duration
        decision_duration = self._decision_duration
        try:
            self._client.register_workflow_type(
                name=self._name,
                version=self._version,
                task_list=self._task_list,
                default_execution_start_to_close_timeout=workflow_duration,
                default_task_start_to_close_timeout=decision_duration,
                default_child_policy='TERMINATE'
            )
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError:
            logger.exception('Error while registering workflow:')
            return False
        return True

    def _check_if_compatible(self):
        try:
            c = self._client.describe_workflow_type(
                workflow_name=self._name,
                workflow_version=self._version
            )['configuration']
        except SWFResponseError:
            logger.exception('Error while checking workflow compatibility:')
            return False
        workflow_duration = self._workflow_duration
        decision_duration = self._decision_duration
        return all([
            c['defaultTaskList']['name'] == self._task_list,
            c['defaultExecutionStartToCloseTimeout'] == workflow_duration,
            c['defaultTaskStartToCloseTimeout'] == decision_duration,
        ])
