import json
import uuid
import logging
from collections import namedtuple
from functools import partial
from pkgutil import simplegeneric

from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions
from boto.swf.exceptions import SWFTypeAlreadyExistsError, SWFResponseError


__all__ = ['Client', 'SWFClient']


class SWFClient(object):
    def __init__(self, domain, client=None):
        self._client = client if client is not None else Layer1()
        self._domain = domain
        self._scheduled_activities = []

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

    def start_workflow(self, name, version, task_list, input):
        try:
            r = self._client.start_workflow_execution(
                domain=self._domain,
                workflow_id=str(uuid.uuid4()),
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

    def poll_decision(self, task_list):
        poller = partial(self._client.poll_for_decision_task,
                         task_list=task_list, domain=self._domain,
                         reverse_order=True)
        paged_poller = partial(_repeated_poller, poller, _decision_page)
        decision_collapsed = _poll_decision_collapsed(paged_poller)
        # Collapsing decisions pages may fail if some pages are unavailable
        if decision_collapsed is None:
            return
        return _decision_response(decision_collapsed)

    def poll_activity(self, task_list):
        poller = partial(self._client.poll_for_activity_task,
                         task_list=task_list, domain=self._domain)
        return _repeated_poller(poller, _activity_response)

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
                'input': input
            }
        ))

    def schedule_activities(self, token, context=None):
        d = Layer1Decisions()
        for args, kwargs in self._scheduled_activities:
            d.schedule_activity_task(*args, **kwargs)
            name, version = args[1:]
            logging.info("Scheduled activity: %s %s", name, version)
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

    def heartbeat(self, token):
        try:
            self._client.record_activity_task_heartbeat(task_token=token)
            logging.info("Sent activity heartbeat: %s", token)
        except SWFResponseError:
            logging.warning("Error when sending activity heartbeat: %s",
                            token, exc_info=1)
            return False
        return True


class DecisionClient(object):
    def __init__(self, client, token, decision_data):
        self._client = client
        self._token = token
        self._decision_data = decision_data

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None,
                       schedule_to_close=None,
                       schedule_to_start=None,
                       start_to_close=None,
                       task_list=None,
                       context=None):
        self._client.queue_activity(
            call_id=call_id,
            name=name,
            version=version,
            input=input,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_list=task_list,
            context=context
        )

    def schedule_activities(self, context=None):
        return self._client.schedule_activities(
            token=self._token, context=self._decision_data.serialize(context)
        )

    def complete(self, result):
        return self._client.complete_workflow(token=self._token, result=result)

    def fail(self, reason):
        return self._client.fail_workflow(token=self._token, reason=reason)


class ActivityTask(object):
    def __init__(self, client, token):
        self._client = client
        self._token = token

    def complete(self, result):
        """ Signal the successful completion of the activity with *result*.

        Returns a boolean indicating the success of the operation.

        """
        return self._client.complete_activity(token=self._token, result=result)

    def fail(self, reason):
        """ Signal the failure of the activity for the specified reason.

        Returns a boolean indicating the success of the operation.

        """
        return self._client.fail_activity(token=self._token, reason=reason)

    def heartbeat(self):
        """ Report that the activity is still making progress.

        Returns a boolean indicating the success of the operation or whether
        the heartbeat exceeded the time it should have taken to report activity
        progress. In the latter case the activity execution should be stopped.

        """
        return self._client.heartbeat(token=self._token)


class Decision(object):
    def __init__(self, client, context, new_events):
        self._client = client
        self._new_events = new_events
        self._context = context

        de = self._dispatch_event = simplegeneric(self._dispatch_event)
        de.register(_ActivityScheduled, self._dispatch_activity_scheduled)
        de.register(_ActivityCompleted, self._dispatch_activity_completed)
        de.register(_ActivityFailed, self._dispatch_activity_failed)
        de.register(_ActivityTimedout, self._dispatch_activity_timedout)

        iu = self._internal_update = simplegeneric(self._internal_update)
        iu.register(_ActivityScheduled, self._internal_activity_scheduled)
        for event in new_events:
            self._internal_update(event)

    def dispatch_new_events(self, obj):
        """ Dispatch the new events to specific *obj* methods.

        The dispatch is done in the order the events happened to the following
        methods of which all are optional::

            obj.activity_scheduled(call_id)
            obj.activity_completed(call_id, result)
            obj.activity_failed(call_id, reason)
            obj.activity_timedout(call_id)

        """
        for event in self._new_events:
            self._dispatch_event(event, obj)

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None,
                       schedule_to_close=None,
                       schedule_to_start=None,
                       start_to_close=None,
                       task_list=None,
                       context=None):
        """ Queue an activity.

        This will schedule a run of a previously registered activity with the
        specified *name* and *version*. The *call_id* is used to assign a
        custom identity to this particular queued activity run inside its own
        workflow history. The queueing is done internally, without having the
        client make any requests yet. The actual scheduling is done by calling
        :meth:`schedule_activities`. The activity will be queued in its default
        task list that was set when it was registered, this can be changed by
        setting a different *task_list* value.

        The activity options specified here, if any, have a higher priority
        than the ones used when the activity was registered. For more
        information about the various arguments see :meth:`register_activity`.

        When queueing an acctivity a custom *context* can be set. It can be
        retrieved later by :meth:`activity_context`.

        """
        call_id = str(call_id)
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
        self._context.set_activity_context(call_id, context)

    def schedule_activities(self, context=None):
        """ Schedules all queued activities.

        All activities previously queued by :meth:`queue_activity` will be
        scheduled within the workflow. An optional textual *context* can be set
        and will be available in subsequent decisions using
        :meth:`global_context`. Returns a boolean indicating the success of
        the operation. The internal collection of scheduled
        activities will always be cleared when calling this method.
        """
        return self._client.schedule_activities(
            self._context.serialize(context)
        )

    def complete(self, result):
        """ Signals the successful completion of the workflow.

        Completes the workflow the *result* value. Returns a boolean indicating
        the success of the operation.

        """
        return self._client.complete(result)

    def fail(self, reason):
        """ Signals the termination of the workflow.

        Terminate the workflow identified by *workflow_id* for the specified
        *reason*. All the workflow activities will be abandoned and the final
        result won't be available.
        The *workflow_id* required here is the one obtained when
        :meth:`start_workflow` was called.
        Returns a boolean indicating the success of the operation.

        """
        return self._client.fail(reason)

    def global_context(self, default=None):
        """ Access the global context that was set by
        :meth:`schedule_activities`.

        """
        return self._context.global_context(default)

    def activity_context(self, call_id, default=None):
        """ Access an activity specific context that was set by
        :meth:`queue_activity`.

        """
        return self._context.activity_context(call_id, default)

    def _dispatch_event(self, event, obj):
        """ Dispatch an event to the proper method of obj. """

    def _dispatch_activity_scheduled(self, event, obj):
        meth = 'activity_scheduled'
        call_id = self._context.event_to_call(event.event_id)
        self._dispatch_if_exists(obj, meth, call_id)

    def _dispatch_activity_completed(self, event, obj):
        meth = 'activity_completed'
        call_id = self._context.event_to_call(event.event_id)
        self._dispatch_if_exists(obj, meth, call_id, event.result)

    def _dispatch_activity_failed(self, event, obj):
        meth = 'activity_failed'
        call_id = self._context.event_to_call(event.event_id)
        self._dispatch_if_exists(obj, meth, call_id, event.reason)

    def _dispatch_activity_timedout(self, event, obj):
        meth = 'activity_timedout'
        call_id = self._context.event_to_call(event.event_id)
        self._dispatch_if_exists(obj, meth, call_id)

    def _dispatch_if_exists(self, obj, method_name, *args):
        getattr(obj, method_name, lambda *args: None)(*args)

    def _internal_update(self, event):
        """ Dispatch an event for internal purposes. """

    def _internal_activity_scheduled(self, event):
        self._context.map_event_to_call(event.event_id, event.call_id)


class JSONDecisionContext(object):
    def __init__(self, context=None):
        self._event_to_call_id = {}
        self._activity_contexts = {}
        self._global_context = None
        if context is not None:
            (self._event_to_call_id,
             self._activity_contexts,
             self._global_context) = json.loads(context)

    def global_context(self, default=None):
        if self._global_context is None:
            return default
        return str(self._global_context)

    def activity_context(self, call_id, default=None):
        if call_id not in self._activity_contexts:
            return default
        return str(self._activity_contexts[call_id])

    def set_activity_context(self, call_id, context):
        self._activity_contexts[call_id] = str(context)

    def map_event_to_call(self, event_id, call_id):
        self._event_to_call_id[event_id] = call_id

    def event_to_call(self, event_id):
        return self._event_to_call_id[event_id]

    def serialize(self, new_global_context=None):
        g = self.global_context()
        if new_global_context is not None:
            g = str(new_global_context)
        return json.dumps((self._event_to_call_id, self._activity_contexts, g))


class JSONDecisionData(object):
    def __init__(self, data, _first_run=False):
        self._context = None
        self._input = data
        if not _first_run:
            self._input, self._context = json.loads(data)

    @classmethod
    def for_first_run(cls, data):
        return cls(data, _first_run=True)

    @property
    def context(self):
        return str(self._context) if self._context is not None else None

    @property
    def input(self):
        return str(self._input)

    def serialize(self, new_context=None):
        context = self.context
        if new_context is not None:
            context = str(new_context)
        return json.dumps((self.input, context))


class Client(object):
    """ A simple wrapper around Boto's SWF Layer1 that provides a cleaner
    interface and some convenience.

    Initialize and bind the client to a *domain*. A custom
    :class:`boto.swf.layer1.Layer1` instance can be sent as the *client*
    argument and it will be used instead of the default one.

    """
    _DecisionData = JSONDecisionData
    _DecisionClient = DecisionClient
    _DecisionContext = JSONDecisionContext
    _Decision = Decision
    _ActivityTask = ActivityTask

    def __init__(self, client):
        self._client = client
        self._workflow_registry = {}
        self._activity_registry = {}

    def register_workflow(self, decision_maker, name, version, task_list,
                          execution_start_to_close=3600,
                          task_start_to_close=60,
                          child_policy='TERMINATE',
                          descr=None):

        """ Register a workflow with the given configuration options.

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
        reg_successful = self._client.register_workflow(
            name=name,
            version=version,
            task_list=task_list,
            execution_start_to_close=execution_start_to_close,
            task_start_to_close=task_start_to_close,
            child_policy=child_policy,
            descr=descr
        )
        if reg_successful:
            self._workflow_registry[(name, version)] = decision_maker
        return reg_successful

    def register_activity(self, activity_runner, name, version, task_list,
                          heartbeat=60, schedule_to_close=420,
                          schedule_to_start=120, start_to_close=300,
                          descr=None):
        """ Register an activity with the given configuration options.

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
        reg_successful = self._client.register_activity(
            name=name,
            version=version,
            task_list=task_list,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            descr=descr
        )
        if reg_successful:
            self._activity_registry[(name, version)] = activity_runner
        return reg_successful

    def dispatch_next_decision(self, task_list):
        """ Poll for the next decision and call the matching runner registered.

        If any runner previsouly registered with :meth:`register_workflow`
        matches the polled decision it will be called with two arguments in
        this order: the input that was used when the workflow was scheduled and
        a :class:`Decision` instance. It returns the matched runner if any or
       ``None``.

        """
        decision_response = self._client.poll_decision(task_list)
        # Polling a decision may fail if some pages are unavailable
        if decision_response is None:
            return
        decision_maker_key = decision_response.name, decision_response.version
        decision_maker = self._workflow_registry.get(decision_maker_key)
        if decision_maker is not None:
            if decision_response.first_run:
                data = self._DecisionData.for_first_run(decision_response.data)
            else:
                data = self._DecisionData(decision_response.data)
            decision_client = self._DecisionClient(
                self._client, decision_response.token, data
            )
            decision_context = self._DecisionContext(data.context)
            decision = self._Decision(decision_client, decision_context,
                                      decision_response.new_events)
            decision_maker(data.input, decision)
            return decision_maker

    def dispatch_next_activity(self, task_list):
        """ Poll for the next activity and call the matching runner registered.

        If any runner previsouly registered with :meth:`register_activity`
        matches the polled activity it will be called with two arguments in
        this order: the input that was used when the activity was scheduled and
        a :class:`ActivityTask` instance. It returns the matched runner if any
        or ``None``.

        """
        activity_response = self._client.poll_activity(task_list)
        activity_runner_key = activity_response.name, activity_response.version
        activity_runner = self._activity_registry.get(activity_runner_key)
        if activity_runner is not None:
            activity_task = self._ActivityTask(
                self._client, activity_response.token
            )
            activity_runner(activity_response.input, activity_task)
            return activity_runner


def _decision_event(event):
    event_type = event['eventType']
    if event_type == 'ActivityTaskScheduled':
        event_id = event['eventId']
        ATSEA = 'activityTaskScheduledEventAttributes'
        call_id = event[ATSEA]['activityId']
        return _ActivityScheduled(event_id, call_id)
    elif event_type == 'ActivityTaskCompleted':
        ATCEA = 'activityTaskCompletedEventAttributes'
        event_id = event[ATCEA]['scheduledEventId']
        result = event[ATCEA]['result']
        return _ActivityCompleted(event_id, result)
    elif event_type == 'ActivityTaskFailed':
        ATFEA = 'activityTaskFailedEventAttributes'
        event_id = event[ATFEA]['scheduledEventId']
        reason = event[ATFEA]['reason']
        return _ActivityFailed(event_id, reason)
    elif event_type == 'ActivityTaskTimedOut':
        ATTOEA = 'activityTaskTimedOutEventAttributes'
        event_id = event[ATTOEA]['scheduledEventId']
        return _ActivityTimedout(event_id)
    elif event_type == 'WorkflowExecutionStarted':
        WESEA = 'workflowExecutionStartedEventAttributes'
        input = event[WESEA]['input']
        return _WorkflowStarted(input)
    elif event_type == 'DecisionTaskCompleted':
        DTCEA = 'decisionTaskCompletedEventAttributes'
        context = event[DTCEA]['executionContext']
        started_by = event[DTCEA]['startedEventId']
        return _DecisionCompleted(started_by, context)


def _decision_page(response, event_maker=_decision_event):
    events = [event_maker(e) for e in response['events']]
    return _DecisionPage(
        name=response['workflowType']['name'],
        version=response['workflowType']['version'],
        token=response['taskToken'],
        next_page_token=response.get('nextPageToken'),
        last_event_id=response.get('previousStartedEventId'),
        events=filter(None, events)
    )


def _repeated_poller(poller, result_klass, retries=-1, **kwargs):
    response = {}
    while 'taskToken' not in response or not response['taskToken']:
        try:
            response = poller(**kwargs)
        except (IOError, SWFResponseError):
            logging.warning("Unknown error when polling.", exc_info=1)
        if retries == 0:
            return
        else:
            retries = max(retries - 1, -1)
    return result_klass(response)


def _poll_decision_collapsed(poller):

    first_page = poller()

    def all_events():
        page = first_page
        while 1:
            for event in page.events:
                yield event
            if page.next_page_token is None:
                break
            # If a workflow is stopped and a decision page fetching fails
            # forever we avoid infinite loops
            p = poller(next_page_token=page.next_page_token, retries=3)
            if p is None:
                return
            assert (
                p.name == page.name
                and p.version == page.version
                and p.token == page.token
                and p.last_event_id == page.last_event_id
            ), 'Inconsistent decision pages.'
            page = p

    return _DecisionCollapsed(name=first_page.name, version=first_page.version,
                              all_events=all_events(), token=first_page.token,
                              last_event_id=first_page.last_event_id)


def _decision_response(decision_collapsed):
    first_run = decision_collapsed.last_event_id == 0
    if first_run:
        # The first decision is always just after a workflow started and at
        # this point this should also be first event in the history but it may
        # not be the only one - there may be also be previos decisions that
        # have timed out.
        all_events = tuple(decision_collapsed.all_events)
        workflow_started = all_events[-1]
        new_events = all_events[:-1]
        assert isinstance(workflow_started, _WorkflowStarted)
        data = workflow_started.input
    else:
        # The workflow had previous decisions completed and we should search
        # for the last one
        new_events = []
        for event in decision_collapsed.all_events:
            if isinstance(event, _DecisionCompleted):
                break
            new_events.append(event)
        else:
            raise AssertionError('Last decision was not found.')
        assert event.started_by == decision_collapsed.last_event_id
        data = event.context

    return _DecisionResponse(
        name=decision_collapsed.name,
        version=decision_collapsed.version,
        token=decision_collapsed.token,
        data=data,
        first_run=first_run,
        # Preserve the order in which the events happend.
        new_events=tuple(reversed(new_events))
    )


def _activity_response(response):
    return _ActivityResponse(
        name=response['activityType']['name'],
        version=response['activityType']['version'],
        input=response['input'],
        token=response['taskToken']
    )


_ActivityResponse = namedtuple('_ActivityResponse', 'name version input token')

_DecisionPage = namedtuple(
    '_DecisionPage',
    ['name', 'version', 'events', 'next_page_token', 'last_event_id', 'token']
)
_DecisionCollapsed = namedtuple(
    '_DecisionCollapsed',
    ['name', 'version', 'all_events', 'last_event_id', 'token']
)
_DecisionResponse = namedtuple(
    '_DecisionResponse',
    ['name', 'version', 'new_events', 'token', 'data', 'first_run']
)

_ActivityScheduled = namedtuple('_ActivityScheduled', ['event_id', 'call_id'])
_ActivityCompleted = namedtuple('_ActivityCompleted', ['event_id', 'result'])
_ActivityFailed = namedtuple('_ActivityFailed', ['event_id', 'reason'])
_ActivityTimedout = namedtuple('_ActivityTimedout', ['event_id'])
_WorkflowStarted = namedtuple('_WorkflowStarted', ['input'])
_DecisionCompleted = namedtuple(
    '_DecisionCompleted',
    ['started_by', 'context']
)


def _str_or_none(maybe_none):
    if maybe_none is not None:
        return str(maybe_none)
