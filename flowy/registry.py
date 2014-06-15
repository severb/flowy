from collections import namedtuple

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError
from boto.swf.layer1_decisions import Layer1Decisions
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
    def __init__(self, activity_id, heartbeat=None, schedule_to_close=None,
                 schedule_to_start=None, start_to_close=None):
        self._activity_id = activity_id
        self._heartbeat = _pos_int_or_none(heartbeat)
        self._schedule_to_close = _pos_int_or_none(schedule_to_close)
        self._schedule_to_start = _pos_int_or_none(schedule_to_start)
        self._start_to_close = _pos_int_or_none(start_to_close)
        _bail_if_zero(
            ('heartbeat', self._heartbeat),
            ('schedule_to_close', self._schedule_to_close),
            ('schedule_to_start', self._schedule_to_start),
            ('start_to_close', self._start_to_close)
        )

    def __eq__(self, other):
        if isinstance(other, ActivitySpec):
            return self._activity_id == other._activity_id
        return self._activity_id == other

    def __hash__(self):
        return hash(self._activity_id)

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(activity_id=%r, heartbeat=%s, schedule_to_close=%s,"
                " schedule_to_start=%s, start_to_close=%s)") % (
                    klass, self._activity_id, self._heartbeat,
                    self._schedule_to_close, self._schedule_to_start,
                    self._start_to_close)


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

    def schedule(self, swf_decisions, call_id, input):
        swf_decisions.schedule_activity_task(
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
            and a['defaultTaskHeartbeatTimeout'] == self._heartbeat
            and
            a['defaultTaskScheduleToCloseTimeout'] == self._schedule_to_close
            and
            a['defaultTaskScheduleToStartTimeout'] == self._schedule_to_start
            and a['defaultTaskStartToCloseTimeout'] == self._start_to_close
        )

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
            ('workflow_duration', workflow_duration)
        )

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

    def schedule(self, swf_client, call_id, input, tags=None):
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

    def restart(self, swf_client, token, input, tags=None):
        swf_decisions = Layer1Decisions()
        # BOTO has a bug in this call when setting the decision_duration
        swf_decisions.continue_as_new_workflow_execution(
            start_to_close_timeout=self._decision_duration,
            execution_start_to_close_timeout=self._workflow_duration,
            task_list=self._task_list,
            input=str(input),
            tag_list=tags)
        try:
            swf_client.return_decision_task_completed(
                task_token=token,
                decisions=swf_decisions._data)
            return True
        except SWFResponseError:
            logger.execption('Error while restarting workflow:')
            return False

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
                default_child_policy='TERMINATE'
            )
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
                workflow_version=self._version
            )['configuration']
        except SWFResponseError:
            logger.exception('Error while checking workflow compatibility:')
            return False
        decision_duration = self._decision_duration
        workflow_duration = self._workflow_duration
        return (
            w['defaultTaskList']['name'] == self._task_list
            and w['defaultTaskStartToCloseTimeout'] == decision_duration
            and w['defaultExecutionStartToCloseTimeout'] == workflow_duration
        )


def _bail_if_zero(*mapping):
    for name, val in mapping:
        if val == '0':
            raise ValueError("The value of %s must be a strictly"
                             " positive integer" % name)


def _pos_int_or_none(val):
    if val is not None:
        return str(max(int(val), 0))
    return None
