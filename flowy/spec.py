from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError
from flowy import logger


class SWFActivitySpec(object):
    def __init__(self, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None):
        self._name = name
        self._version = version
        self._task_list = task_list
        self._heartbeat = heartbeat
        self._schedule_to_close = schedule_to_close
        self._schedule_to_start = schedule_to_start
        self._start_to_close = start_to_close

    def schedule(self, swf_decisions, call_id, input):
        heartbeat, schedule_to_close, schedule_to_start, start_to_close = (
            self._encode_timers()
        )
        swf_decisions.schedule_activity_task(
            str(call_id), str(self._name), str(self._version),
            heartbeat_timeout=heartbeat,
            schedule_to_close_timeout=schedule_to_close,
            schedule_to_start_timeout=schedule_to_start,
            start_to_close_timeout=start_to_close,
            task_list=_str_or_none(self._task_list),
            input=str(input))

    def register_remote(self, swf_client):
        success = True
        registered_as_new = self._try_register_remote(swf_client)
        if not registered_as_new:
            success = self._check_if_compatible(swf_client)
        return success

    def _try_register_remote(self, swf_client):
        heartbeat, schedule_to_close, schedule_to_start, start_to_close = (
            self._encode_timers()
        )
        try:
            swf_client.register_activity_type(
                name=str(self._name),
                version=str(self._version),
                task_list=_str_or_none(self._task_list),
                default_task_heartbeat_timeout=heartbeat,
                default_task_schedule_to_close_timeout=schedule_to_close,
                default_task_schedule_to_start_timeout=schedule_to_start,
                default_task_start_to_close_timeout=start_to_close)
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError:
            logger.exception('Error while registering %r:' % self)
            return False
        return True

    def _check_if_compatible(self, swf_client):
        heartbeat, schedule_to_close, schedule_to_start, start_to_close = (
            self._encode_timers()
        )
        try:
            a = swf_client.describe_activity_type(
                activity_name=self._name,
                activity_version=self._version)['configuration']
        except SWFResponseError:
            logger.exception('Error while checking %r compatibility:' % self)
            return False
        return (
            a['defaultTaskList']['name'] == _str_or_none(self._task_list)
            and a['defaultTaskHeartbeatTimeout'] == heartbeat
            and a['defaultTaskScheduleToCloseTimeout'] == schedule_to_close
            and a['defaultTaskScheduleToStartTimeout'] == schedule_to_start
            and a['defaultTaskStartToCloseTimeout'] == start_to_close)

    @property
    def _key(self):
        return (str(self._name), str(self._version))

    def __eq__(self, other):
        if isinstance(other, SWFActivitySpec):
            return self._key == other._key
        return self._key == other

    def __hash__(self):
        return hash(self._key)

    def _encode_timers(self):
        return (
            _timer_encode(self._heartbeat, 'heartbeat'),
            _timer_encode(self._schedule_to_close, 'schedule_to_close'),
            _timer_encode(self._schedule_to_start, 'schedule_to_start'),
            _timer_encode(self._start_to_close, 'start_to_close'))

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(name=%r, version=%r, task_list=%r, heartbeat=%s,"
                " schedule_to_close=%s, schedule_to_start=%s,"
                " start_to_close=%s)") % (
                    klass, self._name, self._version, self._task_list,
                    self._heartbeat, self._schedule_to_close,
                    self._schedule_to_start, self._start_to_close)


class WorkflowSpec(object):
    def __init__(self, workflow_id, decision_duration=None,
                 workflow_duration=None):
        self._workflow_id = workflow_id
        self._decision_duration = _pos_int_or_none(decision_duration)
        self._workflow_duration = _pos_int_or_none(workflow_duration)
        _bail_if_zero(
            ('decision_duration', decision_duration),
            ('workflow_duration', workflow_duration))

    def __eq__(self, other):
        if isinstance(other, WorkflowSpec):
            return self._workflow_id == other._workflow_id
        return self._workflow_id == other

    def __hash__(self):
        return hash(self._workflow_id)

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(workflow_id=%r, decision_duration=%s,"
                " workflow_duration=%s,") % (
                    klass, self._workflow_id, self._decision_duration,
                    self._workflow_duration)


class SWFWorkflowSpec(WorkflowSpec):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None):
        self._name = str(name)
        self._version = str(version)
        if task_list is not None:
            self._task_list = str(task_list)
        super(SWFActivitySpec, self).__init__(
            SWFTaskId(self._name, self._version),
            decision_duration,
            workflow_duration)

    def start(self, swf_client, call_id, input, tags=None):
        if tags is not None:
            tags = set(map(str, tags))
            if len(tags) > 5:
                raise ValueError('Cannot set more than 5 tags')
        try:
            r = swf_client.start_workflow_execution(
                str(call_id), self._name, self._version,
                task_start_to_close_timeout=self._decision_duration,
                execution_start_to_close_timeout=self._workflow_duration,
                task_list=self._task_list,
                input=str(input),
                tag_list=tags)
        except SWFResponseError:
            logger.exception('Could not start the workflow:')
            return None
        return r['runId']

    def restart(self, swf_decisions, input, tags=None):
        # BOTO has a bug in this call when setting the decision_duration
        swf_decisions.continue_as_new_workflow_execution(
            start_to_close_timeout=self._decision_duration,
            execution_start_to_close_timeout=self._workflow_duration,
            task_list=self._task_list,
            input=str(input),
            tag_list=tags)

    def schedule(self, swf_decisions, call_id, input):
        pass

    def register_remote(self, swf_client):
        success = True
        registered_as_new = self._try_register_remote(swf_client)
        if not registered_as_new:
            success = self._check_if_compatible(swf_client)
        return success

    def _try_register_remote(self, swf_client):
        decision_duration = self._decision_duration
        workflow_duration = self._workflow_duration
        try:
            swf_client.register_workflow_type(
                name=self._name,
                version=self._version,
                task_list=self._task_list,
                default_task_start_to_close_timeout=decision_duration,
                default_execution_start_to_close_timeout=workflow_duration,
                default_child_policy='TERMINATE')
        except SWFTypeAlreadyExistsError:
            return False
        except SWFResponseError:
            logger.exception('Error while registering workflow:')
            return False
        return True

    def _check_if_compatible(self, swf_client):
        try:
            w = swf_client.describe_workflow_type(
                workflow_name=self._name,
                workflow_version=self._version)['configuration']
        except SWFResponseError:
            logger.exception('Error while checking workflow compatibility:')
            return False
        decision_duration = self._decision_duration
        workflow_duration = self._workflow_duration
        return (
            w['defaultTaskList']['name'] == self._task_list
            and w['defaultTaskStartToCloseTimeout'] == decision_duration
            and w['defaultExecutionStartToCloseTimeout'] == workflow_duration)


def _timer_encode(val, name):
    if val is None:
        return None
    val = str(max(int(val), 0))
    if val == '0':
        raise ValueError("The value of %s must be a strictly"
                         " positive integer" % name)


def _tags_encode(tags):
    if tags is not None:
        tags = set(map(str, tags))
        if len(tags) > 5:
            raise ValueError('Cannot set more than 5 tags')


def _str_or_none(val):
    if val is None:
        return None
    return str(val)
