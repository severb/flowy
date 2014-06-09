from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError
from flowy import logger


class LocalRegistry(object):
    def __init__(self):
        self._registry = {}

    def register(self, task_spec):
        return task_spec.register(self._register_task)

    def _register_task(self, task_id, task_factory):
        self._registry[task_id] = task_factory

    def __call__(self, task_id, input, scheduler, token):
        try:
            return self._registry[task_id](input, scheduler, token)
        except KeyError:
            return lambda: None


class ActivitySpec(object):
    def __init__(self, task_id, task_factory, heartbeat, schedule_to_close,
                 schedule_to_start, start_to_close):
        self._task_id = task_id
        self._task_factory = task_factory
        self._heartbeat = _pos_int_or_none(heartbeat)
        self._schedule_to_close = _pos_int_or_none(schedule_to_close)
        self._schedule_to_start = _pos_int_or_none(schedule_to_start)
        self._start_to_close = _pos_int_or_none(start_to_close)
        _bail_if_zero([
            ('heartbeat', self._heartbeat),
            ('schedule_to_close', self._schedule_to_close),
            ('schedule_to_start', self._schedule_to_start),
            ('start_to_close', self._start_to_close)
        ])

    def register(self, registry):
        registry(self._task_id, self)
        return True

    def __call__(self, input, scheduler, token):
        return self._task_factory(input, scheduler, token)


class SWFActivitySpec(ActivitySpec):
    def __init__(self, name, version, task_list, task_factory, heartbeat,
                 schedule_to_close, schedule_to_start, start_to_close):
        self._name = str(name)
        self._version = str(version)
        self._task_list = str(task_list)
        super(SWFActivitySpec, self).__init__(
            (self._name, self._version), task_factory, heartbeat,
            schedule_to_close, schedule_to_start, start_to_close
        )


class RemoteSWFActivitySpec(SWFActivitySpec):
    def __init__(self, client, name, version, task_list, task_factory,
                 heartbeat, schedule_to_close, schedule_to_start,
                 start_to_close):
        self._client = client
        super(RemoteSWFActivitySpec, self).__init__(
            name, version, task_list, task_factory, heartbeat,
            schedule_to_close, schedule_to_start, start_to_close
        )

    def register(self, registry):
        success = True
        registered_as_new = self._try_register_remote()
        if not registered_as_new:
            success = self._check_if_compatible()
        if success:
            return super(RemoteSWFActivitySpec, self).register(registry)
        return success

    def _try_register_remote(self):
        try:
            self._client.register_activity_type(
                name=self._name,
                version=self._version,
                task_list=self._task_list,
                default_task_heartbeat_timeout=self._heartbeat,
                default_task_schedule_to_close_timeout=self._schedule_to_close,
                default_task_schedule_to_start_timeout=self._schedule_to_start,
                default_task_start_to_close_timeout=self._start_to_close
            )
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError:
            logger.exception('Error while registering activity:')
            return False
        return True

    def _check_if_compatible(self, name, version):
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


def _bail_if_zero(mapping):
    for name, val in mapping:
        if val == '0':
            raise ValueError("The value of %s must be a strictly"
                             " positive integer" % name)


def _pos_int_or_none(val):
    if val is not None:
        return str(max(int(val), 0))
    return None
