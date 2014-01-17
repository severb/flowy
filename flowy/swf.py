import json
import logging
import uuid
from collections import namedtuple
from functools import partial
from itertools import ifilter, imap
from pkgutil import simplegeneric

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError
from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions

__all__ = ['StatefulJobDispatcher', 'SWFClient']


class SWFClient(object):
    def __init__(self, domain, client=None):
        self._client = client if client is not None else Layer1()
        self._domain = domain
        self._scheduled_activities = []
        self._scheduled_workflows = []
        self._scheduled_timers = []
        self._scheduled_restart = None

    def register_workflow(self, name, version, task_list,
                          execution_start_to_close=3600,
                          task_start_to_close=60,
                          child_policy='TERMINATE',
                          descr=None):
        v = str(version)
        estc = str(execution_start_to_close)
        tstc = str(task_start_to_close)
        try:
            self._client.register_workflow_type(
                domain=self._domain,
                name=name,
                version=v,
                task_list=task_list,
                default_child_policy=child_policy,
                default_execution_start_to_close_timeout=estc,
                default_task_start_to_close_timeout=tstc,
                description=descr
            )
            logging.info("Registered workflow: %s %s", name, version)
        except SWFTypeAlreadyExistsError:
            logging.warning("Workflow already registered: %s %s",
                            name, version)
            try:
                reg_w = self._client.describe_workflow_type(
                    domain=self._domain, workflow_name=name, workflow_version=v
                )
            except SWFResponseError:
                logging.warning("Could not check workflow defaults: %s %s",
                                name, version)
                return False
            conf = reg_w['configuration']
            reg_estc = conf['defaultExecutionStartToCloseTimeout']
            reg_tstc = conf['defaultTaskStartToCloseTimeout']
            reg_tl = conf['defaultTaskList']['name']
            reg_cp = conf['defaultChildPolicy']

            if (reg_estc != estc
                    or reg_tstc != tstc
                    or reg_tl != task_list
                    or reg_cp != child_policy):
                logging.warning("Registered workflow "
                                "has different defaults: %s %s",
                                name, version)
                return False
        except SWFResponseError:
            logging.warning("Could not register workflow: %s %s",
                            name, version, exc_info=1)
            return False
        return True

    def register_activity(self, name, version, task_list, heartbeat=60,
                          schedule_to_close=420, schedule_to_start=120,
                          start_to_close=300, descr=None):
        version = str(version)
        schedule_to_close = str(schedule_to_close)
        schedule_to_start = str(schedule_to_start)
        start_to_close = str(start_to_close)
        heartbeat = str(heartbeat)
        try:
            self._client.register_activity_type(
                domain=self._domain,
                name=name,
                version=version,
                task_list=task_list,
                default_task_heartbeat_timeout=heartbeat,
                default_task_schedule_to_close_timeout=schedule_to_close,
                default_task_schedule_to_start_timeout=schedule_to_start,
                default_task_start_to_close_timeout=start_to_close,
                description=descr
            )
            logging.info("Registered activity: %s %s", name, version)
        except SWFTypeAlreadyExistsError:
            logging.warning("Activity already registered: %s %s",
                            name, version)
            try:
                reg_a = self._client.describe_activity_type(
                    domain=self._domain, activity_name=name,
                    activity_version=version
                )
            except SWFResponseError:
                logging.warning("Could not check activity defaults: %s %s",
                                name, version)
                return False
            conf = reg_a['configuration']
            reg_tstc = conf['defaultTaskStartToCloseTimeout']
            reg_tsts = conf['defaultTaskScheduleToStartTimeout']
            reg_tschtc = conf['defaultTaskScheduleToCloseTimeout']
            reg_hb = conf['defaultTaskHeartbeatTimeout']
            reg_tl = conf['defaultTaskList']['name']

            if (reg_tstc != start_to_close
                    or reg_tsts != schedule_to_start
                    or reg_tschtc != schedule_to_close
                    or reg_hb != heartbeat
                    or reg_tl != task_list):
                logging.warning("Registered activity "
                                "has different defaults: %s %s",
                                name, version)
                return False
        except SWFResponseError:
            logging.warning("Could not register activity: %s %s",
                            name, version, exc_info=1)
            return False
        return True

    def start_workflow(self, name, version, task_list, input,
                       workflow_id=None):
        if workflow_id is None:
            workflow_id = uuid.uuid4()
        try:
            r = self._client.start_workflow_execution(
                domain=self._domain,
                workflow_id=str(workflow_id),
                workflow_name=name,
                workflow_version=str(version),
                task_list=task_list,
                input=input
            )
        except SWFResponseError:
            logging.warning("Could not start workflow: %s %s",
                            name, version, exc_info=1)
            return None
        return r['runId']

    def poll_decision(self, task_list, event_factory=None, page_retry=3):
        if event_factory is None:
            event_factory = _event_factory
        poller = partial(self._client.poll_for_decision_task,
                         task_list=task_list, domain=self._domain,
                         reverse_order=True)

        first_page = _repeated_poller(poller)

        def all_events():
            page = first_page
            while 1:
                for event in page['events']:
                    yield event
                if not page.get('nextPageToken'):
                    break
                # If a workflow is stopped and a decision page fetching fails
                # forever we avoid infinite loops
                p = _repeated_poller(
                    poller,
                    next_page_token=page['nextPageToken'],
                    retries=page_retry
                )
                if p is None:
                    raise PageError()
                assert (
                    p['taskToken'] == page['taskToken']
                    and (
                        p['workflowType']['name']
                        == page['workflowType']['name'])
                    and (
                        p['workflowType']['version']
                        == page['workflowType']['version'])
                    and (
                        p.get('previousStartedEventId')
                        == page.get('previousStartedEventId')
                    )
                ), 'Inconsistent decision pages.'
                page = p

        return DecisionResponse(
            name=first_page['workflowType']['name'],
            version=first_page['workflowType']['version'],
            token=first_page['taskToken'],
            last_event_id=first_page.get('previousStartedEventId'),
            events_iter=ifilter(None, imap(event_factory, all_events()))
        )

    def poll_activity(self, task_list):
        poller = partial(self._client.poll_for_activity_task,
                         task_list=task_list, domain=self._domain)
        response = _repeated_poller(poller)
        return ActivityResponse(
            name=response['activityType']['name'],
            version=response['activityType']['version'],
            input=response['input'],
            token=response['taskToken']
        )

    def restart_workflow(self,
                         task_start_to_close=None,
                         execution_start_to_close=None,
                         task_list=None,
                         input=None,
                         workflow_type_version=None):
        if self._scheduled_restart is None:
            self._scheduled_restart = {
                'start_to_close_timeout': task_start_to_close,
                'execution_start_to_close_timeout': execution_start_to_close,
                'task_list': task_list,
                'input': input,
                'workflow_type_version': workflow_type_version,
            }

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None,
                       schedule_to_close=None,
                       schedule_to_start=None,
                       start_to_close=None,
                       task_list=None,
                       context=None):
        self._scheduled_activities.append((
            (str(call_id), name, str(version)),
            {
                'heartbeat_timeout': _str_or_none(heartbeat),
                'schedule_to_close_timeout': _str_or_none(schedule_to_close),
                'schedule_to_start_timeout': _str_or_none(schedule_to_start),
                'start_to_close_timeout': _str_or_none(start_to_close),
                'task_list': task_list,
                'input': input,
                'control': context,
            }
        ))

    def queue_subworkflow(self, workflow_id, name, version, input,
                          task_start_to_close=None,
                          execution_start_to_close=None,
                          task_list=None,
                          context=None):
        self._scheduled_workflows.append((
            (name, str(version), str(workflow_id)),
            {
                'execution_start_to_close_timeout': execution_start_to_close,
                'task_start_to_close_timeout': task_start_to_close,
                'task_list': task_list,
                'input': input,
                'control': context,
            }
        ))

    def queue_timer(self, call_id, delay, context=None):
        self._scheduled_timers.append((str(delay), str(call_id), context))

    def schedule_queued(self, token, context=None):
        d = Layer1Decisions()
        if self._scheduled_restart is not None:
            d.continue_as_new_workflow_execution(**self._scheduled_restart)
            logging.info("Scheduled workflow restart")
        else:
            for args, kwargs in self._scheduled_activities:
                d.schedule_activity_task(*args, **kwargs)
                name, version = args[1:]
                logging.info("Scheduled activity: %s %s", name, version)
            for args, kwargs in self._scheduled_workflows:
                d.start_child_workflow_execution(*args, **kwargs)
                name, version = args[:2]
                logging.info("Scheduled child workflow: %s %s", name, version)
            for args in self._scheduled_timers:
                d.start_timer(*args)
        data = d._data
        try:
            self._client.respond_decision_task_completed(
                task_token=token, decisions=data, execution_context=context
            )
        except SWFResponseError:
            logging.warning("Could not send decisions: %s", token, exc_info=1)
            return False
        finally:
            self._scheduled_activities = []
            self._scheduled_workflows = []
            self._scheduled_timers = []
            self._scheduled_restart = None
        return True

    def complete_workflow(self, token, result=None):
        d = Layer1Decisions()
        d.complete_workflow_execution(result=result)
        data = d._data
        try:
            self._client.respond_decision_task_completed(
                task_token=token, decisions=data
            )
            logging.info("Completed workflow: %s %s", token, result)
        except SWFResponseError:
            logging.warning("Could not complete the workflow: %s",
                            token, exc_info=1)
            return False
        return True

    def fail_workflow(self, token, reason):
        d = Layer1Decisions()
        d.fail_workflow_execution(reason=reason)
        data = d._data
        try:
            self._client.respond_decision_task_completed(
                task_token=token, decisions=data
            )
            logging.info("Terminated workflow: %s", reason)
        except SWFResponseError:
            logging.warning("Could not fail the workflow: %s",
                            token, exc_info=1)
            return False
        return True

    def complete_activity(self, token, result):
        try:
            self._client.respond_activity_task_completed(
                task_token=token, result=result
            )
            logging.info("Completed activity: %s %r", token, result)
        except SWFResponseError:
            logging.warning("Could not complete activity: %s",
                            token, exc_info=1)
            return False
        return True

    def fail_activity(self, token, reason):
        try:
            self._client.respond_activity_task_failed(task_token=token,
                                                      reason=reason)
            logging.info("Failed activity: %s %s", token, reason)
        except SWFResponseError:
            logging.warning("Could not terminate activity: %s",
                            token, exc_info=1)
            return False
        return True

    def heartbeat(self, token, details):
        try:
            self._client.record_activity_task_heartbeat(
                task_token=token,
                details=json.dumps(details)
            )
            logging.info("Sent activity heartbeat: %s", token)
        except SWFResponseError:
            logging.warning("Error when sending activity heartbeat: %s",
                            token, exc_info=1)
            return False
        return True


DecisionResponse = namedtuple(
    'DecisionResponse',
    'name version events_iter last_event_id token'
)

ActivityResponse = namedtuple(
    'ActivityResponse',
    'name version input token'
)


class PageError(RuntimeError):
    """ Raised when a page in a decision response is unavailable. """


def _repeated_poller(poller, retries=-1, **kwargs):
    response = {}
    try:
        response = poller(**kwargs)
    except (IOError, SWFResponseError):
        logging.warning("Unknown error when polling.", exc_info=1)
    while 'taskToken' not in response or not response['taskToken']:
        if retries == 0:
            return
        try:
            response = poller(**kwargs)
        except (IOError, SWFResponseError):
            logging.warning("Unknown error when polling.", exc_info=1)
        retries = max(retries - 1, -1)
    return response


def _make_event_factory(event_map):
    tuples = {}
    for event_class_name, attrs in event_map.values():
        tuples[event_class_name] = namedtuple(event_class_name, attrs.keys())

    globals().update(tuples)

    def factory(event):
        event_type = event['eventType']
        if event_type in event_map:
            event_class_name, attrs = event_map[event_type]
            kwargs = {}
            for attr_name, attr_path in attrs.items():
                attr_value = event
                for attr_path_part in attr_path.split('.'):
                    attr_value = attr_value.get(attr_path_part)
                kwargs[attr_name] = attr_value
            event_class = tuples.get(event_class_name, lambda **k: None)
            return event_class(**kwargs)
        return None

    return factory


# Dynamically create all the event tuples and a factory for them
_event_factory = _make_event_factory({
    # Activities

    'ActivityTaskScheduled': ('ActivityScheduled', {
        'event_id': 'eventId',
        'call_id': 'activityTaskScheduledEventAttributes.activityId',
        'context': 'activityTaskScheduledEventAttributes.control',
    }),
    'ActivityTaskCompleted': ('ActivityCompleted', {
        'event_id': 'activityTaskCompletedEventAttributes.scheduledEventId',
        'result': 'activityTaskCompletedEventAttributes.result',
    }),
    'ActivityTaskFailed': ('ActivityFailed', {
        'event_id': 'activityTaskFailedEventAttributes.scheduledEventId',
        'reason': 'activityTaskFailedEventAttributes.reason',
    }),
    'ActivityTaskTimedOut': ('ActivityTimedout', {
        'event_id': 'activityTaskTimedOutEventAttributes.scheduledEventId',
    }),
    'ScheduleActivityTaskFailed': ('PreActivityFail', {
        'call_id': 'scheduleActivityTaskFailedEventAttributes.activityId',
        'reason': 'scheduleActivityTaskFailedEventAttributes.cause',
    }),

    # Subworkflows

    'StartChildWorkflowExecutionInitiated': ('SubworkflowStarted', {
        'event_id': 'startChildWorkflowExecutionInitiatedEventAttributes'
                    '.workflowId',
        'context': 'startChildWorkflowExecutionInitiatedEventAttributes'
                   '.control'
    }),
    'ChildWorkflowExecutionCompleted': ('SubworkflowCompleted', {
        'event_id': 'childWorkflowExecutionCompletedEventAttributes'
                    '.workflowExecution.workflowId',
        'result': 'childWorkflowExecutionCompletedEventAttributes.result',
    }),
    'ChildWorkflowExecutionFailed': ('SubworkflowFailed', {
        'event_id': 'childWorkflowExecutionFailedEventAttributes'
                    '.workflowExecution.workflowId',
        'reason': 'childWorkflowExecutionFailedEventAttributes.reason',
    }),
    'StartChildWorkflowExecutionFailed': ('PreSubworkflowFail', {
        'event_id': 'startChildWorkflowExecutionFailedEventAttributes'
                    '.workflowId',
        'reason': 'startChildWorkflowExecutionFailedEventAttributes.cause',
    }),
    'ChildWorkflowExecutionTimedOut': ('SubworkflowTimedout', {
        'event_id': 'childWorkflowExecutionTimedOutEventAttributes'
                    '.workflowExecution.workflowId',
    }),

    # Timers

    'TimerStarted': ('TimerStarted', {
        'call_id': 'timerStartedEventAttributes.timerId',
        'context': 'timerStartedEventAttributes.control',
    }),
    'TimerFired': ('TimerFired', {
        'call_id': 'timerFiredEventAttributes.timerId',
    }),

    # Misc

    'WorkflowExecutionStarted': ('WorkflowStarted', {
        'input': 'workflowExecutionStartedEventAttributes.input',
    }),
    'DecisionTaskCompleted': ('DecisionCompleted', {
        'context': 'decisionTaskCompletedEventAttributes.executionContext',
        'started_by': 'decisionTaskCompletedEventAttributes.startedEventId',
    }),
    'DecisionTaskStarted': ('DecisionStarted', {
        'event_id': 'eventId'
    }),
})


class ActivityTask(object):
    def __init__(self, client, token):
        self._client = client
        self._token = token

    def complete(self, result):
        """ Trigger the successful completion of the activity with *result*.

        Return a boolean indicating the success of the operation.

        """
        return self._client.complete_activity(token=self._token, result=result)

    def fail(self, reason):
        """ Trigger the failure of the activity for the specified reason.

        Return a boolean indicating the success of the operation.

        """
        return self._client.fail_activity(token=self._token, reason=reason)

    def heartbeat(self, details=None):
        """ Report that the activity is still making progress.

        Return a boolean indicating the success of the operation or whether the
        heartbeat exceeded the time it should have taken to report activity
        progress. In the latter case the activity execution should be stopped
        after an optional cleanup.

        """
        return self._client.heartbeat(token=self._token, details=details)


class StatefulDecision(object):
    """ A workflow decision that can be created from a previous state.

    This class allows easy queueing of new activities, workflows and timers and
    at the same time it provides ways to query the history of the execution. In
    order to recreate the execution history it only requires the *new_events*
    and its *previous_state* if any. The *client* and *token* are used to
    forward the actions to the right workflow instance.

    """
    def __init__(self, client, new_events, token, previous_state=None):
        self._client = client
        # Cache the events in case of an iterator because we may need to walk
        # over it multiple times
        self._new_events = tuple(new_events)
        self._token = token
        self._contexts = {}
        self._to_call_id = {}
        self._running = set()
        self._timedout = set()
        self._results = {}
        self._errors = {}
        self._fired = set()
        self._global_context = None
        self._is_finished = False

        if previous_state is not None:
            self._load(previous_state)

        self._setup_internal_dispatch()
        self._update(new_events)

    def _setup_internal_dispatch(self):
        iu = self._internal_update = simplegeneric(self._internal_update)
        iu.register(ActivityScheduled, self._activity_scheduled)  # noqa
        iu.register(ActivityCompleted, self._job_completed)  # noqa
        iu.register(ActivityFailed, self._job_failed)  # noqa
        iu.register(ActivityTimedout, self._job_timedout)  # noqa
        iu.register(SubworkflowStarted, self._subworkflow_started)  # noqa
        iu.register(SubworkflowCompleted, self._job_completed)  # noqa
        iu.register(SubworkflowFailed, self._job_failed)  # noqa
        iu.register(SubworkflowTimedout, self._job_timedout)  # noqa
        iu.register(PreSubworkflowFail, self._pre_subworkflow_fail)  # noqa
        iu.register(PreActivityFail, self._pre_activity_fail)  # noqa
        iu.register(TimerStarted, self._timer_started)  # noqa
        iu.register(TimerFired, self._timer_fired)  # noqa

    def _update(self, new_events):
        for event in self._new_events:
            self._internal_update(event)

    def _internal_update(self, event):
        """ Dispatch an event for internal purposes. """

    def _activity_scheduled(self, event):
        self._to_call_id[event.event_id] = event.call_id
        self._running.add(event.call_id)

    def _subworkflow_started(self, event):
        call_id = self._to_call_id[event.event_id]
        self._running.add(call_id)

    def _job_completed(self, event):
        call_id = self._to_call_id[event.event_id]
        assert call_id not in self._results
        assert call_id not in self._errors
        assert call_id not in self._timedout
        self._running.remove(call_id)
        self._results[call_id] = event.result

    def _job_failed(self, event):
        call_id = self._to_call_id[event.event_id]
        assert call_id not in self._results
        assert call_id not in self._errors
        assert call_id not in self._timedout
        self._running.remove(call_id)
        self._errors[call_id] = event.reason

    def _job_timedout(self, event):
        call_id = self._to_call_id[event.event_id]
        assert call_id not in self._results
        assert call_id not in self._errors
        assert call_id not in self._timedout
        self._running.remove(call_id)
        self._timedout.add(call_id)

    def _pre_subworkflow_fail(self, event):
        call_id = self._to_call_id[event.event_id]
        assert call_id not in self._results
        assert call_id not in self._errors
        assert call_id not in self._timedout
        # When a job is not found it's not even started
        assert call_id not in self._running
        self._errors[call_id] = event.reason

    def _pre_activity_fail(self, event):
        assert event.call_id not in self._results
        assert event.call_id not in self._errors
        assert event.call_id not in self._timedout
        # When a job is not found it's not even started
        assert event.call_id not in self._running
        self._errors[event.call_id] = event.reason

    def _timer_started(self, event):
        self._running.add(event.call_id)

    def _timer_fired(self, event):
        self._running.remove(event.call_id)
        self._fired.add(event.call_id)

    def _check_call_id(self, call_id):
        if (
            call_id in self._running
            or call_id in self._results
            or call_id in self._errors
            or call_id in self._timedout
            or call_id in self._fired
        ):
            raise RuntimeError("Value %s was already used for a"
                               " different call_id." % call_id)

    def restart_workflow(self,
                         task_start_to_close=None,
                         execution_start_to_close=None,
                         task_list=None,
                         input=None,
                         workflow_type_version=None):
        self._client.restart_workflow(
            task_start_to_close=task_start_to_close,
            execution_start_to_close=execution_start_to_close,
            task_list=task_list,
            input=input,
            workflow_type_version=workflow_type_version,
        )

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None,
                       schedule_to_close=None,
                       schedule_to_start=None,
                       start_to_close=None,
                       task_list=None,
                       context=None):
        """ Queue an activity.

        Schedule a run of the previously registered activity with the specified
        *name* and *version* passing the given *input*. The *call_id* is used
        to assign a custom identity to this particular queued activity run
        inside its own workflow history. It must be unique among all queued
        activities, subworkflows and timers queued in this particular workflow.

        The activity will be queued with its default arguments that were set
        when it was registered - this can be changed by setting custom values
        for *heartbeat*, *schedule_to_close*, *schedule_to_start*,
        *start_to_close* and *task_list* arguments. The activity options
        specified here, if any, have a higher priority than the ones used when
        the activity was registered. For more information about the various
        arguments see :meth:`CachingClient.register_activity`.

        When queueing an activity a custom *context* can be set. It can be
        retrieved later or in a future decision of the same workflow using
        :meth:`context`.

        Some of the methods like :meth:`is_scheduled`, :meth:`is_timeout`,
        :meth:`get_result` and :meth:`get_error` can be used to query the
        status of a particular activity run identified by *call_id*.

        """
        call_id = str(call_id)
        self._check_call_id(call_id)
        self._client.queue_activity(
            call_id=call_id,
            name=name,
            version=version,
            input=input,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_list=task_list
        )
        if context is not None:
            self._contexts[call_id] = str(context)

    def queue_subworkflow(self, call_id, name, version, input,
                          task_start_to_close=None,
                          execution_start_to_close=None,
                          task_list=None,
                          context=None):
        """ Queue a subworkflow.

        Just like :meth:`queue_activity` but this method schedules an entire
        workflow run with different options that can be overridden:
        *task_start_to_close*, *execution_start_to_close* and *task_list*.
        Similarly a *context* can be passed and later retrieved using
        :meth:`context`.

        """
        call_id = str(call_id)
        self._check_call_id(call_id)
        workflow_id = str(uuid.uuid4())
        self._client.queue_subworkflow(
            workflow_id=workflow_id,
            name=name,
            version=version,
            input=input,
            task_start_to_close=task_start_to_close,
            execution_start_to_close=execution_start_to_close,
            task_list=task_list
        )
        self._to_call_id[workflow_id] = call_id
        if context is not None:
            self._contexts[call_id] = str(context)

    def queue_timer(self, call_id, delay, context=None):
        """ Queue a timer.

        Start a timer identified by *call_id* that will fire after *delay*
        seconds. The *call_id* must be a unique among all activities,
        subworkflows and timers queued in this workflow. The state of the timer
        can be queried by passing the *call_id* to :meth:`is_scheduled` and
        :meth:`is_fired`.

        """
        call_id = str(call_id)
        self._check_call_id(call_id)
        self._client.queue_timer(call_id=call_id, delay=delay)
        if context is not None:
            self._contexts[call_id] = str(context)

    def complete(self, result):
        """ Trigger the successful completion of the workflow.

        Complete the workflow with the *result* value and return a boolean
        indicating the success of the operation.

        """
        if self._is_finished:
            return
        self._is_finished = True
        return self._client.complete_workflow(token=self._token,
                                              result=str(result))

    def fail(self, reason):
        """ Trigger the termination of the workflow.

        Terminate the workflow identified by *workflow_id* for the specified
        *reason*. When a workflow is terminated all the activities will be
        abandoned and the final result won't be available. Return a boolean
        indicating the success of the operation.

        """
        if self._is_finished:
            return
        self._is_finished = True
        return self._client.fail_workflow(token=self._token,
                                          reason=str(reason))

    def context(self, call_id, default=None):
        """ Query the context of a particular activity, workflow or timer. """
        return self._contexts.get(call_id, default)

    def is_finished(self):
        """ Check if any of :meth:`complete` or :meth:`fail` was called. """
        return self._is_finished

    def is_scheduled(self, call_id):
        """ Query if a particular activity, workflow or timer is scheduled.

        This method returns a `True` value only as long as the queried job is
        scheduled and not any other state like completed, fired, etc.

        """
        return call_id in self._running

    def is_fired(self, call_id):
        """ Query if a timer fired. """
        return call_id in self._fired

    def get_result(self, call_id, default=None):
        """ Get the result for an activity or workflow if available. """
        return self._results.get(call_id, default)

    def get_error(self, call_id, default=None):
        """ Get the error for an activity or workflow if available. """
        return self._errors.get(call_id, default)

    def is_timeout(self, call_id):
        """ Check if an activity or workflow timedout. """
        return call_id in self._timedout

    def override_global_context(self, context=None):
        """ Override the global decision context.

        Similarly with setting a custom context for a specific job a global
        context can be set and retrieved in later decisions of the same
        workflow run using :meth:`global_context`.

        """
        self._global_context = str(context)

    def global_context(self):
        """ Retrieve the global decision context. """
        return self._global_context

    def dump_state(self):
        """ Return a textual representation of the current state. """
        return _str_concat(json.dumps((
            self._contexts,
            # json makes int keys as strings
            list(self._to_call_id.items()),
            list(self._running),
            list(self._timedout),
            self._results,
            self._errors,
            list(self._fired),
        )), self._global_context)

    def _load(self, data):
        json_data, self._global_context = _str_deconcat(data)
        (self._contexts,
         to_call_id,
         running,
         timedout,
         self._results,
         self._errors,
         fired) = json.loads(json_data)
        self._to_call_id = dict(to_call_id)
        self._running = set(running)
        self._timedout = set(timedout)
        self._fired = set(fired)


class StatefulJobDispatcher(object):
    """ Register and dispatch decisions and activities to a callable.

    This dispatcher will instantiate decisions by passing only the new events
    available in the history together with the state of the previous decision.
    This approach is very limited by the max size of the context a workflow can
    have, so it's only useful for short workflows composed of jobs with
    relatively small results but minimizes a lot the amount of history
    pagination needed.

    """

    ActivityTask = ActivityTask
    Decision = StatefulDecision

    def __init__(self, client_maker):
        self._client_maker = client_maker
        self._workflow_registry = {}
        self._activity_registry = {}

    def register_workflow(self, decision_maker, name, version, task_list,
                          execution_start_to_close=3600,
                          task_start_to_close=60,
                          child_policy='TERMINATE',
                          descr=None, register_remote=True):

        """ Register *decision_maker* callable to handle this workflow type.

        If a workflow with the same *name* and *version* is already registered,
        this method returns a boolean indicating whether the registered
        workflow is compatible. A compatible workflow is a workflow that was
        registered using the same default values. The default total workflow
        running time can be specified in seconds using
        *execution_start_to_close* and a specific decision task runtime can be
        limited by setting *task_start_to_close*. The default task list the
        workflows of this type will be scheduled on can be set with
        *task_list*.

        """
        version = str(version)
        client = self._client_maker()
        reg_result = True
        if register_remote:
            reg_result = client.register_workflow(
                name=name,
                version=version,
                task_list=task_list,
                execution_start_to_close=execution_start_to_close,
                task_start_to_close=task_start_to_close,
                child_policy=child_policy,
                descr=descr
            )
        if reg_result:
            self._workflow_registry[(name, version)] = decision_maker
        return reg_result

    def register_activity(self, activity_runner, name, version, task_list,
                          heartbeat=60, schedule_to_close=420,
                          schedule_to_start=120, start_to_close=300,
                          descr=None, register_remote=True):
        """ Register *activity_runner* callable to handle this activity type.

        If an activity with the same *name* and *version* is already
        registered, this method returns a boolean indicating whether the
        registered activity is compatible. A compatible activity is an
        activity that was registered using the same default values.
        The allowed running time can be specified in seconds using
        *start_to_close*, the allowed time from the moment it was scheduled
        to the moment it finished can be specified using *schedule_to_close*
        and the time it can spend in the queue before the processing itself
        starts can be specified using *schedule_to_start*. The default task
        list the activities of this type will be scheduled on can be set with
        *task_list*.

        """
        version = str(version)
        client = self._client_maker()
        reg_result = True
        if register_remote:
            reg_result = client.register_activity(
                name=name,
                version=version,
                task_list=task_list,
                heartbeat=heartbeat,
                schedule_to_close=schedule_to_close,
                schedule_to_start=schedule_to_start,
                start_to_close=start_to_close,
                descr=descr
            )
        if reg_result:
            self._activity_registry[(name, version)] = activity_runner
        return reg_result

    def start_workflow(self, name, version, task_list, input,
                       workflow_id=None):
        client = self._client_maker()
        return client.start_workflow(
            name=name,
            version=version,
            task_list=task_list,
            input=input,
            workflow_id=workflow_id,
        )

    def dispatch_next_decision(self, task_list):
        """ Poll for the next decision and call the matching runner registered.

        If any runner previously registered with :meth:`register_workflow`
        matches the polled decision it will be called with two arguments in
        this order: the input that was used when the workflow was scheduled and
        a :class:`Decision` instance. It returns the matched runner if any or
       ``None``.

        """
        client = self._client_maker()
        decision_response = client.poll_decision(task_list)
        # Polling a decision may fail if some pages are unavailable
        if decision_response is None:
            return

        decision_maker_key = decision_response.name, decision_response.version
        decision_maker = self._workflow_registry.get(decision_maker_key)
        if decision_maker is None:
            return

        first_run = decision_response.last_event_id == 0
        if first_run:
            # The first decision is always just after a workflow started and at
            # this point this should also be first event in the history but it
            # may not be the only one - there may be also be previous decisions
            # that have timed out.
            try:
                all_events = tuple(decision_response.events_iter)
            except PageError:
                return  # Not all pages were available
            workflow_started = all_events[-1]
            new_events = all_events[:-1]
            assert isinstance(workflow_started, WorkflowStarted)  # noqa
            input, context_data = workflow_started.input, None
        else:
            # The workflow had previous decisions completed and we should
            # search for the last one
            new_events = []
            try:
                for event in decision_response.events_iter:
                    if isinstance(event, DecisionStarted):  # noqa
                        if event.event_id == decision_response.last_event_id:
                            break
                    new_events.append(event)
                else:
                    assert False, 'Last decision started was not found.'
            except PageError:
                return
            for event in new_events:
                if isinstance(event, DecisionCompleted):  # noqa
                    assert event.started_by == decision_response.last_event_id
                    input, context_data = _str_deconcat(event.context)
                    break
            else:
                assert False, 'Last decision completed was not found.'

        decision = self.Decision(client, reversed(new_events),
                                 decision_response.token, context_data)

        decision_maker(input, decision)

        if not decision.is_finished():
            client.schedule_queued(decision_response.token,
                                   _str_concat(input, decision.dump_state()))

        return decision_maker

    def dispatch_next_activity(self, task_list):
        """ Poll for the next activity and call the matching runner registered.

        If any runner previously registered with :meth:`register_activity`
        matches the polled activity it will be called with two arguments in
        this order: the input that was used when the activity was scheduled and
        a :class:`ActivityTask` instance. It returns the matched runner if any
        or ``None``.

        """
        client = self._client_maker()
        activity_response = client.poll_activity(task_list)
        activity_runner_key = activity_response.name, activity_response.version
        activity_runner = self._activity_registry.get(activity_runner_key)
        if activity_runner is not None:
            activity_task = self.ActivityTask(client, activity_response.token)
            activity_runner(activity_response.input, activity_task)
            return activity_runner


def _str_or_none(maybe_none):
    if maybe_none is not None:
        return str(maybe_none)
    return None


def _str_concat(str1, str2=None):
    str1 = str(str1)
    if str2 is None:
        return '%d %s' % (len(str1), str1)
    return '%d %s%s' % (len(str1), str1, str2)


def _str_deconcat(s):
    str1_len, str1str2 = s.split(' ', 1)
    str1_len = int(str1_len)
    str1, str2 = str1str2[:str1_len], str1str2[str1_len:]
    if str2 == '':
        str2 = None
    return str1, str2
