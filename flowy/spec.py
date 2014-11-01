import logging
from collections import namedtuple
from contextlib import contextmanager
from functools import total_ordering

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError


logger = logging.getLogger(__name__)


_sentinel = object()
_SWFKeyTuple = namedtuple('_SWFKeyTuple', 'name version')


def SWFSpecKey(name, version):
    return _SWFKeyTuple(str(name), str(version))


@total_ordering  # make the registration deterministic
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
            self._timers_encode())
        swf_decisions.schedule_activity_task(
            str(call_id), str(self._name), str(self._version),
            heartbeat_timeout=heartbeat,
            schedule_to_close_timeout=schedule_to_close,
            schedule_to_start_timeout=schedule_to_start,
            start_to_close_timeout=start_to_close,
            task_list=_str_or_none(self._task_list),
            input=str(input))

    @contextmanager
    def options(self, task_list=_sentinel, heartbeat=_sentinel,
                schedule_to_close=_sentinel, schedule_to_start=_sentinel,
                start_to_close=_sentinel):
        old_task_list = self._task_list
        old_heartbeat = self._heartbeat
        old_schedule_to_close = self._schedule_to_close
        old_schedule_to_start = self._schedule_to_start
        old_start_to_close = self._start_to_close
        if task_list is not _sentinel:
            self._task_list = task_list
        if heartbeat is not _sentinel:
            self._heartbeat = heartbeat
        if schedule_to_close is not _sentinel:
            self._schedule_to_close = schedule_to_close
        if schedule_to_start is not _sentinel:
            self._schedule_to_start = schedule_to_start
        if start_to_close is not _sentinel:
            self._start_to_close = start_to_close
        yield
        self._task_list = old_task_list
        self._heartbeat = old_heartbeat
        self._schedule_to_close = old_schedule_to_close
        self._schedule_to_start = old_schedule_to_start
        self._start_to_close = old_start_to_close

    def register_remote(self, swf_client):
        success = True
        registered_as_new = self._try_register_remote(swf_client)
        if not registered_as_new:
            success = self._check_if_compatible(swf_client)
        return success

    def _try_register_remote(self, swf_client):
        heartbeat, schedule_to_close, schedule_to_start, start_to_close = (
            self._timers_encode())
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
            logger.exception('Error while registering %s:', self)
            return False
        return True

    def _check_if_compatible(self, swf_client):
        try:
            a = swf_client.describe_activity_type(
                activity_name=str(self._name),
                activity_version=str(self._version))['configuration']
        except SWFResponseError:
            logger.exception('Error while checking %s compatibility:', self)
            return False
        heartbeat, schedule_to_close, schedule_to_start, start_to_close = (
            self._timers_encode())
        task_list = _str_or_none(self._task_list)
        return (
            a.get('defaultTaskList', {}).get('name') == task_list
            and a.get('defaultTaskHeartbeatTimeout') == heartbeat
            and a.get('defaultTaskScheduleToCloseTimeout') == schedule_to_close
            and a.get('defaultTaskScheduleToStartTimeout') == schedule_to_start
            and a.get('defaultTaskStartToCloseTimeout') == start_to_close)

    @property
    def _key(self):
        return SWFSpecKey(self._name, self._version)

    def __eq__(self, other):
        if isinstance(other, SWFActivitySpec):
            return self._key == other._key
        return self._key == other

    def __lt__(self, other):
        if isinstance(other, SWFActivitySpec):
            return self._key < other._key
        return self._key < other

    def __hash__(self):
        return hash(self._key)

    def _timers_encode(self):
        return (
            _timer_encode(self._heartbeat, 'heartbeat'),
            _timer_encode(self._schedule_to_close, 'schedule_to_close'),
            _timer_encode(self._schedule_to_start, 'schedule_to_start'),
            _timer_encode(self._start_to_close, 'start_to_close'))

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(name=%r, version=%r, task_list=%r, heartbeat=%r,"
                " schedule_to_close=%r, schedule_to_start=%r,"
                " start_to_close=%r)") % (
                    klass, self._name, self._version, self._task_list,
                    self._heartbeat, self._schedule_to_close,
                    self._schedule_to_start, self._start_to_close)


@total_ordering
class SWFWorkflowSpec(object):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None):
        self._name = name
        self._version = version
        self._task_list = task_list
        self._decision_duration = decision_duration
        self._workflow_duration = workflow_duration

    def start(self, swf_client, call_id, input, tags=None):
        decision_duration, workflow_duration = self._timers_encode()
        try:
            r = swf_client.start_workflow_execution(
                str(call_id), str(self._name), str(self._version),
                task_start_to_close_timeout=decision_duration,
                execution_start_to_close_timeout=workflow_duration,
                task_list=_str_or_none(self._task_list),
                input=str(input),
                tag_list=_tags_encode(tags))
        except SWFResponseError:
            logger.exception('Error while starting the workflow:')
            return None
        return r['runId']

    def restart(self, swf_decisions, input, tags=None):
        decision_duration, workflow_duration = self._timers_encode()
        # BOTO has a bug in this call when setting the decision_duration
        swf_decisions.continue_as_new_workflow_execution(
            start_to_close_timeout=decision_duration,
            execution_start_to_close_timeout=workflow_duration,
            task_list=_str_or_none(self._task_list),
            input=str(input),
            tag_list=_tags_encode(tags))
        CANWEDA = 'continueAsNewWorkflowExecutionDecisionAttributes'
        last_decision_attrs = swf_decisions._data[-1][CANWEDA]
        STCT, TSTCT = 'startToCloseTimeout', 'taskStartToCloseTimeout'
        if STCT in last_decision_attrs:
            last_decision_attrs[TSTCT] = last_decision_attrs.pop(STCT)

    def schedule(self, swf_decisions, call_id, input):
        decision_duration, workflow_duration = self._timers_encode()
        swf_decisions.start_child_workflow_execution(
            str(self._name), str(self._version), str(call_id),
            task_start_to_close_timeout=decision_duration,
            execution_start_to_close_timeout=workflow_duration,
            task_list=_str_or_none(self._task_list),
            input=str(input)
        )

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel):
        old_task_list = self._task_list
        old_decision_duration = self._decision_duration
        old_workflow_duration = self._workflow_duration
        if task_list is not _sentinel:
            self._task_list = task_list
        if decision_duration is not _sentinel:
            self._decision_duration = decision_duration
        if workflow_duration is not _sentinel:
            self._workflow_duration = workflow_duration
        yield
        self._task_list = old_task_list
        self._decision_duration = old_decision_duration
        self._workflow_duration = old_workflow_duration

    def register_remote(self, swf_client):
        success = True
        registered_as_new = self._try_register_remote(swf_client)
        if not registered_as_new:
            success = self._check_if_compatible(swf_client)
        return success

    def _try_register_remote(self, swf_client):
        decision_duration, workflow_duration = self._timers_encode()
        try:
            swf_client.register_workflow_type(
                name=str(self._name),
                version=str(self._version),
                task_list=_str_or_none(self._task_list),
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
                workflow_name=str(self._name),
                workflow_version=str(self._version))['configuration']
        except SWFResponseError:
            logger.exception('Error while checking workflow compatibility:')
            return False
        decision_duration, wf_duration = self._timers_encode()
        task_list = _str_or_none(self._task_list)
        return (
            w.get('defaultTaskList', {}).get('name') == task_list
            and w.get('defaultTaskStartToCloseTimeout') == decision_duration
            and w.get('defaultExecutionStartToCloseTimeout') == wf_duration)

    @property
    def _key(self):
        return SWFSpecKey(self._name, self._version)

    def __eq__(self, other):
        if isinstance(other, SWFWorkflowSpec):
            return self._key == other._key
        return self._key == other

    def __lt__(self, other):
        if isinstance(other, SWFActivitySpec):
            return self._key < other._key
        return self._key < other

    def __hash__(self):
        return hash(self._key)

    def _timers_encode(self):
        return (
            _timer_encode(self._decision_duration, 'decision_duration'),
            _timer_encode(self._workflow_duration, 'workflow_duration'))

    def __repr__(self):
        klass = self.__class__.__name__
        return ("%s(name=%r, version=%r, task_list=%r, decision_duration=%r,"
                " workflow_duration=%r)") % (
                    klass, self._name, self._version, self._task_list,
                    self._decision_duration, self._workflow_duration)


def _timer_encode(val, name):
    if val is None:
        return None
    val = max(int(val), 0)
    if val == 0:
        raise ValueError("The value of %s must be a strictly"
                         " positive integer" % name)
    return str(val)


def _tags_encode(tags):
    if tags is not None:
        # make it deterministic for tests
        tags = sorted(set(map(str, tags)))
        if len(tags) > 5:
            raise ValueError('Cannot set more than 5 tags')
    return tags


def _str_or_none(val):
    if val is None:
        return None
    return str(val)
