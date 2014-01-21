from boto.swf.exceptions import SWFResponseError

from flowy.spec import RemoteTaskSpec


class SWFActivitySpec(RemoteTaskSpec):
    def __init__(self, task_id, task_factory, client, task_list,
                 heartbeat=60,
                 schedule_to_close=420,
                 schedule_to_start=120,
                 start_to_close=300,
                 description=None):
        super(SWFActivitySpec, self).__init__(
            task_id=task_id,
            task_factory=task_factory,
            client=client
        )
        self._name = str(task_id.name)
        self._version = str(task_id.version)
        self._task_list = str(task_list)
        self._heartbeat = str(heartbeat)
        self._schedule_to_close = str(schedule_to_close)
        self._schedule_to_start = str(schedule_to_start)
        self._start_to_close = str(start_to_close)
        self._description = None
        if description is not None:
            self._description = str(description)

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
                description=self._description
            )
        except SWFResponseError:  # SWFTypeAlreadyExistsError is subclass
            return False
        return True

    def _check_if_compatible(self):
        try:
            c = self._client.describe_activity_type(
                activity_name=self._name,
                activity_version=self._version
            )['configuration']
        except SWFResponseError:
            return False
        return all([
            c['defaultTaskList']['name'] == self._task_list,
            c['defaultTaskHeartbeatTimeout'] == self._heartbeat,
            c['defaultTaskScheduleToCloseTimeout'] == self._schedule_to_close,
            c['defaultTaskScheduleToStartTimeout'] == self._schedule_to_start,
            c['defaultTaskStartToCloseTimeout'] == self._start_to_close
        ])
