from collections import namedtuple

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError
from flowy import logger


class LocalRegistry(object):
    def __init__(self):
        self._registry = {}

    def register(self, task_spec, task_factory):
        self._registry[task_spec] = task_factory

    def __call__(self, task_spec, input, scheduler, token):
        try:
            return self._registry[task_spec](input, scheduler, token)
        except KeyError:
            return lambda: None


class ActivitySpec(object):
    def __init__(self, task_id, heartbeat, schedule_to_close,
                 schedule_to_start, start_to_close):
        self._task_id = task_id
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

    def __eq__(self, other):
        if isinstance(other, ActivitySpec):
            return self._task_id == other._task_id
        return self._task_id == other

    def __hash__(self):
        return hash(self._task_id)

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(task_id=%r, heartbeat=%s, schedule_to_close=%s,"
                " schedule_to_start=%s, start_to_close=%s)") % (
            klass, self._task_id, self._heartbeat, self._schedule_to_close,
            self._schedule_to_start, self._start_to_close
        )


SWFTaskId = namedtuple('SWFTaskId', 'name version')


class SWFActivitySpec(ActivitySpec):
    def __init__(self, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None):
        self._name = str(name)
        self._version = str(version)
        self._task_list = task_list
        if task_list is not None:
            self._task_list = str(task_list)
        super(SWFActivitySpec, self).__init__(
            SWFTaskId(self._name, self._version), heartbeat, schedule_to_close,
            schedule_to_start, start_to_close
        )

    def schedule(self, swf_client, call_id, input):
        self.swf_client.schedule_activity_task(
            str(call_id), self._name, self._version,
            heartbeat_timeout=self._heartbeat,
            schedule_to_close_timeout=self._schedule_to_close,
            schedule_to_start_timeout=self._schedule_to_start,
            start_to_close_timeout=self._start_to_close,
            task_list=self._task_list,
            input=str(input)
        )

    def register_remote(self, swf_client):
        success = True
        registered_as_new = self._try_register_remote(swf_client)
        if not registered_as_new:
            success = self._check_if_compatible(swf_client)
        return success

    def _try_register_remote(self, swf_client):
        try:
            swf_client.register_activity_type(
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
            logger.exception('Error while registering %r:' % self)
            return False
        return True

    def _check_if_compatible(self, swf_client):
        try:
            a = swf_client.describe_activity_type(
                activity_name=self._name,
                activity_version=self._version
            )['configuration']
        except SWFResponseError:
            logger.exception('Error while checking %r compatibility:' % self)
            return False
        return (
            a['defaultTaskList']['name'] == self._task_list
            and
            a['defaultTaskHeartbeatTimeout'] == self._heartbeat
            and
            a['defaultTaskScheduleToCloseTimeout'] == self._schedule_to_close
            and
            a['defaultTaskScheduleToStartTimeout'] == self._schedule_to_start
            and
            a['defaultTaskStartToCloseTimeout'] == self._start_to_close
        )

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(name=%r, version=%r, task_list=%r, heartbeat=%s,"
                " schedule_to_close=%s, schedule_to_start=%s,"
                " start_to_close=%s)") % (
            klass, self._name, self._version, self._task_list, self._heartbeat,
            self._schedule_to_close, self._schedule_to_start,
            self._start_to_close
        )


def _bail_if_zero(mapping):
    for name, val in mapping:
        if val == '0':
            raise ValueError("The value of %s must be a strictly"
                             " positive integer" % name)


def _pos_int_or_none(val):
    if val is not None:
        return str(max(int(val), 0))
    return None
