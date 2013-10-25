import sys
import json
import uuid
import logging
from collections import namedtuple
from itertools import chain
from pkgutil import simplegeneric

from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions
from boto.swf.exceptions import SWFTypeAlreadyExistsError, SWFResponseError

from flowy.workflow import _UnhandledActivityError


__all__ = ['ActivityClient', 'WorkflowClient']


class Decision(object):
    """ A decision that must be taken every time the workflow state changes.

    Initializing this class with a *client* will block until a decision task
    will be successfully polled from the *task_list*. Every decision has access
    to the entire workflow execution history. This class provides an API for
    the interesting parts of the workflow execution history and for managing
    the context. The context is a persistent customisable part of the workflow
    execution history.

    This class also wraps the workflow specific funtionality of
    :class:`SWFClient` and automatically forwards some of the
    information about the workflow for which a decision is needed like the
    ``token`` value.

    """
    def __init__(self, name, version, token, client,
                 input=None, context=None, new_events=[]):
        self.name = name
        self.version = version
        self._token = token
        self._client = client
        self._scheduled_activities = []
        self._event_to_call_id = {}
        self._running = set()
        self._results = {}
        self._timed_out = set()
        self._activity_ctx = {}
        self._errors = {}
        self.input = input

        if context is not None:
            self.input, history, self.context = json.loads(context)
            self._event_to_call_id = history['id_mapping']
            self._running = history['running']
            self._results = history['results']
            self._timed_out = history['timed_out']
            self._errors = history['errors']
            self._activity_ctx = history['activity_ctx']

        reg = self._register = simplegeneric(self._register)
        reg.register(SWFActivityScheduled, self._register_activity_scheduled)
        reg.register(SWFActivityCompleted, self._register_activity_completed)
        reg.register(SWFActivityFailed, self._register_activity_failed)
        reg.register(SWFActivityTimedout, self._register_activity_timedout)
        for event in new_events:
            reg(event)

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
        retrieved later uisg :meth:`activity_context`.

        """
        call_id = str(call_id)
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
        if context is not None:
            self._activity_ctx[call_id] = context

    def schedule_activities(self, context=None):
        """ Schedules all queued activities.

        All activities previously queued by :meth:`queue_activity` will be
        scheduled within the workflow. An optional textual *context* can be
        set and will be available in subsequent decisions as :attr:`context`.
        Returns a boolean indicating the success of the operation. On success
        the internal collection of scheduled activities will be cleared.
        """
        d = Layer1Decisions()
        for args, kwargs in self._scheduled_activities:
            d.schedule_activity_task(*args, **kwargs)
            name, version = args[1:]
            logging.info("Scheduled activity: %s %s", name, version)
        data = d._data
        c = self.context
        if context is not None:
            c = context
        history = {
            'id_mapping': self._event_to_call_id,
            'running': self._running,
            'results': self._results,
            'timed_out': self._timed_out,
            'errors': self._errors,
            'activity_ctx': self._activity_ctx,
        }
        try:
            self._client.respond_decision_task_completed(
                task_token=self._token,
                decisions=data,
                execution_context=json.dumps((self.input, history, c))
            )
        except SWFResponseError:
            logging.warning("Could not send decisions: %s",
                            self.token, exc_info=1)
            return False
        self._scheduled_activities = []
        return True

    def complete_workflow(self, result):
        """ Signals the successful completion of the workflow.

        Completes the workflow the *result* value. Returns a boolean indicating
        the success of the operation.

        """
        d = Layer1Decisions()
        d.complete_workflow_execution(result=result)
        data = d._data
        try:
            self._client.respond_decision_task_completed(task_token=self.token,
                                                         decisions=data)
            logging.info("Completed workflow: %s %s", self.token, result)
        except SWFResponseError:
            logging.warning("Could not complete workflow: %s",
                            self.token, exc_info=1)
            return False
        return True

    def terminate_workflow(self, workflow_id, reason):
        """ Signals the termination of the workflow.

        Terminate the workflow identified by *workflow_id* for the specified
        *reason*. All the workflow activities will be abandoned and the final
        result won't be available.
        The *workflow_id* required here is the one obtained when
        :meth:`start_workflow` was called.
        Returns a boolean indicating the success of the operation.

        """
        try:
            self._client.terminate_workflow_execution(domain=self.domain,
                                                      workflow_id=workflow_id,
                                                      reason=reason)
            logging.info("Terminated workflow: %s %s", workflow_id, reason)
        except SWFResponseError:
            logging.warning("Could not terminate workflow: %s",
                            workflow_id, exc_info=1)
            return False
        return True

    def any_activity_running(self):
        """ Checks the history for any activities running.

        See :meth:`is_activity_running`.

        """
        return bool(self._running)

    def is_activity_running(self, call_id):
        """ Checks whether the activity with *call_id* is running.

        Any activities that have been scheduled but not yet completed are
        considered to be running.

        """
        return call_id in self._running

    def activity_result(self, call_id, default=None):
        """ Return the result for the activity identified by *call_id*
        with an optional *default* value.

        """
        call_id = str(call_id)
        return self._results.get(call_id, default)

    def activity_error(self, call_id, default=None):
        """ Return the reason why the activity identified by *call_id* failed
        or a *default* one if no such reason is found.

        """
        call_id = str(call_id)
        return self._errors.get(call_id, default)

    def is_activity_timedout(self, call_id):
        """ Check whether the activity identified by *call_id* timed out. """
        call_id = str(call_id)
        return call_id in self._timed_out

    def _register(self, event):
        pass

    def _register_activity_scheduled(self, event):
        event_id = str(event.event_id)
        call_id = str(event.call_id)
        self._event_to_call_id[event_id] = call_id
        self._running.add(call_id)
        if call_id in self._timed_out:
            self._timed_out.remove(call_id)

    def _register_activity_completed(self, event):
        event_id = str(event.event_id)
        call_id = self._event_to_call_id[event_id]
        self._running.remove(call_id)
        self._results[call_id] = event.result

    def _register_activity_failed(self, event):
        event_id = str(event.event_id)
        call_id = self._event_to_call_id[event_id]
        self._running.remove(call_id)
        self._errors[call_id] = event.reason

    def _register_activity_timedout(self, event):
        event_id = str(event.event_id)
        call_id = self._event_to_call_id[event_id]
        self._running.remove(call_id)
        self._timed_out.add(call_id)


class ActivityTask(object):
    """ An object that abstracts an activity and its functionality. """
    def __init__(self, name, version, input, token, client):
        self.name = name
        self.version = version
        self.input = input
        self._token = token
        self._client = client

    def complete(self, result):
        """ Signals the successful completion of an activity.

        Completes the activity with the *result* value. Returns a boolean
        indicating the success of the operation.

        """
        try:
            self._client.respond_activity_task_completed(
                task_token=self._token, result=result
            )
            logging.info("Completed activity: %s %s", self._token, result)
        except SWFResponseError:
            logging.warning("Could not complete activity: %s",
                            self._token, exc_info=1)
            return False
        return True

    def terminate_activity(self, reason):
        """ Signals the termination of the activity.

        Terminate the activity for the specified *reason*. Returns a boolean
        indicating the success of the operation.

        """
        try:
            self._client.respond_activity_task_failed(task_token=self._token,
                                                      reason=reason)
            logging.info("Terminated activity: %s %s", self._token, reason)
        except SWFResponseError:
            logging.warning("Could not terminate activity: %s",
                            self._token, exc_info=1)
            return False
        return True

    def heartbeat(self):
        """ Report that the activity is still making progress.

        Returns a boolean indicating the success of the operation or whether
        the heartbeat exceeded the time it should have taken to report activity
        progress. In the latter case the activity execution should be stopped.

        """
        try:
            self._client.record_activity_task_heartbeat(task_token=self._token)
            logging.info("Sent activity heartbeat: %s", self._token)
        except SWFResponseError:
            logging.warning("Error when sending activity heartbeat: %s",
                            self._token, exc_info=1)
            return False
        return True


SWFActivityScheduled = namedtuple('SWFActivityScheduled', 'event_id call_id')
SWFActivityCompleted = namedtuple('SWFActivityCompleted', 'event_id result')
SWFActivityFailed = namedtuple('SWFActivityFailed', 'event_id reason')
SWFActivityTimedout = namedtuple('SWFActivityTimedout', 'event_id')


class SWFClient(object):
    """ A simple wrapper around Boto's SWF Layer1 that provides a cleaner
    interface and some convenience.

    Initialize and bind the client to a *domain*. A custom
    :class:`boto.swf.layer1.Layer1` instance can be sent as the *client*
    argument and it will be used instead of the default one.

    """
    def __init__(self, domain, client=None):
        self._client = client if client is not None else Layer1()
        self.domain = domain
        self._scheduled_activities = []

    def register_workflow(self, name, version, task_list,
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
        v = str(version)
        estc = str(execution_start_to_close)
        tstc = str(task_start_to_close)
        try:
            self._client.register_workflow_type(
                domain=self.domain,
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
                    domain=self.domain, workflow_name=name, workflow_version=v
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

    def poll_decision(self, task_list, decision_factory=Decision):
        """ Poll for a new decision task.

        Blocks until a decision is available in the *task_list*. A decision is
        a :class:`Decision` instance.

        """
        def poll_page(next_page_token=None):
            poll = self._client.poll_for_decision_task
            response = {}
            while 'taskToken' not in response or not response['taskToken']:
                try:
                    response = poll(domain=self.domain, task_list=task_list,
                                    reverse_order=True,
                                    next_page_token=next_page_token)
                except (IOError, SWFResponseError):
                    logging.warning("Unknown error when pulling"
                                    " decisions: %s %s",
                                    self.domain, task_list, exc_info=1)
            return response

        def poll_all_pages():
            page = poll_page()
            yield page
            while 'nextPageToken' in page:
                page = poll_page(next_page_token=page['nextPageToken'])

        def all_events(pages):
            for page in pages:
                for event in page['events']:
                    yield event

        def new_events(prev_id, events):
            result = []
            for event in events:
                if event['eventType'] == 'DecisionTaskCompleted':
                    assert event['eventId'] == prev_id
                    break
                result.append(event)
            return reversed(result), event['executionContext']

        def typed_events(events):
            for event in events:
                event_type = event['eventType']
                if event_type == 'ActivityTaskScheduled':
                    ATSEA = 'activityTaskScheduledEventAttributes'
                    event_id = event['eventId']
                    call_id = event[ATSEA]['activityId']
                    yield SWFActivityScheduled(event_id, call_id)
                elif event_type == 'ActivityTaskCompleted':
                    ATCEA = 'activityTaskCompletedEventAttributes'
                    event_id = event[ATCEA]['scheduledEventId']
                    result = event[ATCEA]['result']
                    yield SWFActivityCompleted(event_id, result)
                elif event_type == 'ActivityTaskFailed':
                    ATFEA = 'activityTaskFailedEventAttributes'
                    event_id = event[ATFEA]['scheduledEventId']
                    reason = event[ATFEA]['reason']
                    yield SWFActivityFailed(event_id, reason)
                elif event_type == 'ActivityTaskTimedOut':
                    ATTOEA = 'activityTaskTimedOutEventAttributes'
                    event_id = event[ATTOEA]['scheduledEventId']
                    yield SWFActivityTimedout(event_id)

        all_pages = poll_all_pages()
        first_page = all_pages.next()
        all_pages = chain([first_page], all_pages)

        name = first_page['workflowType']['name']
        version = first_page['workflowType']['version']
        token = first_page['taskToken']
        input = None
        context = None

        events = all_events(all_pages)

        prev_id = first_page.get('previousStartedEventId')
        if prev_id:
            events, context = new_events(prev_id, events)
        else:
            first_event = events.next()
            assert first_event['eventType'] == 'WorkflowExecutionStarted'
            WESEA = 'workflowExecutionStartedEventAttributes'
            input = first_event[WESEA]['input']

        new_events = tuple(typed_events(events))  # cache the result

        return decision_factory(name=name, version=version, token=token,
                                client=self._client, context=context,
                                input=input, new_events=new_events)

    def register_activity(self, name, version, task_list, heartbeat=60,
                          schedule_to_close=420, schedule_to_start=120,
                          start_to_close=300, descr=None):
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
        schedule_to_close = str(schedule_to_close)
        schedule_to_start = str(schedule_to_start)
        start_to_close = str(start_to_close)
        heartbeat = str(heartbeat)
        try:
            self._client.register_activity_type(
                domain=self.domain,
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
                    domain=self.domain, activity_name=name,
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

    def poll_activity(self, task_list, activity_factory=ActivityTask):
        """ Poll for a new activity task.

        Blocks until an activity is available in the *task_list* and returns
        it.

        """
        poll = self._client.poll_for_activity_task
        response = {}
        while 'taskToken' not in response or not response['taskToken']:
            try:
                response = poll(domain=self.domain, task_list=task_list)
            except (IOError, SWFResponseError):
                logging.warning("Unknown error when pulling activities: %s %s",
                                self.domain, task_list, exc_info=1)

        name = response['activityType']['name']
        version = response['activityType']['version']
        input = response['input']
        token = response['taskToken']

        return activity_factory(name=name, version=version, input=input,
                                token=token, client=self._client)

    def start_workflow(self, name, version, task_list, input):
        """ Starts the workflow identified by *name* and *version* with the
        given *input* on *task_list*.

        Returns the ``workflow_id`` that can be used to uniquely identify the
        workflow execution within a domain. If starting the execution
        encounters an error, ``None`` is returned. The returned
        ``workflow_id`` can be used when calling :meth:`terminate_workflow`.

        """
        try:
            r = self._client.start_workflow_execution(self.domain,
                                                      str(uuid.uuid4()),
                                                      name, str(version),
                                                      task_list=task_list,
                                                      input=input)
        except SWFResponseError:
            logging.warning("Could not start workflow: %s %s",
                            name, version, exc_info=1)
            return None
        return r['runId']


class WorkflowClient(object):
    """ The object responsible for managing workflows.

    A workflow is registered either manually with the :meth:`register` method
    or using an instance of this class as a decorator. In addition any
    arguments used for registration can be passed to the decorator as keyword
    arguments - additional arguments that aren't used for registration will be
    used to instantiate the ``Workflow`` implementation::

    >>> client = WorkflowClient()
    >>> 
    >>> @client(name="MyWorkflow", version=1, task_list='mylist', x=2)
    >>> class MyWorkflow(Workflow):
    >>> 
    >>>     def __init__(self, x, y=3):
    >>>         pass
    >>> 
    >>>     def run(self):
    >>>         pass

    When the client is started using the :meth:`start` method, it starts the
    main loop polling for decisions that need to be handled, matching them
    based on their name and version.

    """
    def __init__(self):
        self._workflows = {}
        self._register_queue = []

    def register(self, name, version, task_list, workflow_runner,
                 execution_start_to_close=3600, task_start_to_close=60,
                 child_policy='TERMINATE', doc=None):
        """ Register a workflow with the given *name*, *value* and defaults.

        """
        self._workflows[(name, str(version))] = workflow_runner
        self._register_queue.append((name, version, task_list,
                                     execution_start_to_close,
                                     task_start_to_close, child_policy, doc))

    def start_on(self, domain, task_list, client=None):
        """ A shortcut for :meth:`start`.

        Start the main loop in the given *domain*, *task_list* and an optional
        :class:`boto.swf.layer1.Layer1` *client*.

        """
        client = SWFClient(domain, client=client)
        return self.start(client, task_list)

    def start(self, client, task_list):
        """ Starts the main loop polling on *task_list* and using a specific
        :class:`SWFClient` *client*.

        Calling this method will start the loop responsible for polling
        decisions, matching a runner on the names and versions used with
        :meth:`register`, scheduling any activities that should be scheduled,
        and completing or terminating the workflow if needed.
        The loop runs until there are no more activity tasks that need to be
        scheduled, or an exception is encountered.

        """
        for args in self._register_queue:
            if not client.register_workflow(*args):
                sys.exit(1)
        while 1:
            decision = client.next_decision(task_list)
            logging.info("Processing workflow: %s %s",
                         decision.name, decision.version)
            workflow_runner = self._query(decision.name, decision.version)
            if workflow_runner is None:
                logging.warning("No workflow registered for: %s %s",
                                decision.name, decision.version)
                continue
            try:
                result, activities = workflow_runner.resume(
                    decision.input, decision
                )
            except _UnhandledActivityError as e:
                logging.warning("Stopped workflow because of an exception"
                                " inside an activity: %s", e.message)
                decision.terminate_workflow(e.message)
            except Exception as e:
                logging.warning("Stopped workflow because of an unhandled"
                                " exception: %s", e.message)
                decision.terminate_workflow(e.message)
            else:
                activities_running = decision.any_activity_running()
                activities_scheduled = bool(activities)
                if activities_running or activities_scheduled:
                    for a in activities:
                        decision.queue_activity(
                            a.call_id, a.name, a.version, a.input,
                            heartbeat=a.options.heartbeat,
                            schedule_to_close=a.options.schedule_to_close,
                            schedule_to_start=a.options.schedule_to_start,
                            start_to_close=a.options.start_to_close,
                            task_list=a.options.task_list,
                            retries=a.options.retry
                        )
                    decision.schedule_activities()
                else:
                    decision.complete_workflow(result)

    def __call__(self, name, version, task_list, *args, **kwargs):
        optional_args = [
            'execution_start_to_close',
            'task_start_to_close',
            'child_policy',
        ]
        r_kwargs = {}
        for arg_name in optional_args:
            arg_value = kwargs.pop(arg_name, None)
            if arg_value is not None:
                r_kwargs[arg_name] = arg_value

        def wrapper(workflow):
            r_kwargs['doc'] = workflow.__doc__.strip()
            self.register(name, version, task_list, workflow(*args, **kwargs),
                          **r_kwargs)
            return workflow

        return wrapper

    def scheduler_on(self, domain, name, version, task_list, client=None):
        """ A shortcut for :meth:`scheduler`.

        An optional :class:`boto.swf.layer1.Layer1` can be set as *client*.

        """
        c = SWFClient(domain, client=client)
        return self.scheduler(name, version, task_list, c)

    def scheduler(self, name, version, task_list, client):
        """ Create a scheduler for the workflow with the *name* and *version*
        on the specific *task_list* using an instance of :class:`SWFClient` as
        *client*.

        This method returns a function that can be used to trigger the
        scheduling of a workflow with the previously defined attributes::

        >>> my_wf = wclient.scheduler('MyWorkflow', 1, 'mylist', swfclient)
        >>> my_wf(1, x=0)
        >>> my_wf(2, x=0) # Note: schedules two executions of this workflow

        """
        def wrapper(*args, **kwargs):
            input = self.serialize_workflow_arguments(*args, **kwargs)
            return client.start_workflow(name, version, task_list, input)

        return wrapper

    @staticmethod
    def serialize_workflow_arguments(*args, **kwargs):
        """ Serialize the given arguments. """
        return json.dumps({"args": args, "kwargs": kwargs})

    def _query(self, name, version):
        return self._workflows.get((name, version))


class ActivityClient(object):
    """ The object responsible for managing the activity runs.

    Activities are registered either manually with the :meth:`register` method
    or using an instance of this class as a decorator. In addition any
    arguments used for registration can be passed to the decorator as keyword
    arguments - additional arguments not used for registration will be used to
    instantiate the ``Activity`` implementation::

    >>> client = ActivityClient()
    >>> 
    >>> @client(name='MyActivity', version=1, heartbeat=120, x=1, y=2)
    >>> class MyActivity(Activity):
    >>> 
    >>>     def __init__(self, x, y=1):
    >>>         pass
    >>> 
    >>>     def run(self):
    >>>         pass

    When the client is started using the :meth:`start` method, it starts the
    main loop polling for activities that need to be ran, matching them based
    on their name and version and executing them.

    """
    def __init__(self):
        self._activities = {}
        self._register_queue = []

    def register(self, name, version, task_list, activity_runner,
                 heartbeat=60, schedule_to_close=420, schedule_to_start=120,
                 start_to_close=300, doc=None):
        """ Register an activity with the given *name*, *value* and defaults.

        """
        # All versions are converted to string in SWF and that's how we should
        # store them too in order to be able to query for them
        self._activities[(name, str(version))] = activity_runner
        self._register_queue.append((name, version, task_list, heartbeat,
                                     schedule_to_close, schedule_to_start,
                                     start_to_close, doc))

    def start_on(self, domain, task_list, client=None):
        """ A shortcut for :meth:`start`.

        Start the main loop in the given *domain*, *task_list* and an optional
        :class:`boto.swf.layer1.Layer1` *client*.

        """
        client = SWFClient(domain, client=client)
        return self.start(client, task_list)

    def start(self, client, task_list):
        """ Starts the main loop polling on *task_list* using a specific
        :class:`SWFClient` *client*.

        Calling this method will start the loop responsible for polling
        activities, matching them on the names and versions used
        with :meth:`register` and running them.

        """
        for args in self._register_queue:
            if not client.register_activity(*args):
                sys.exit(1)
        while 1:
            response = client.next_activity(task_list)
            logging.info("Processing activity: %s %s",
                         response.name, response.version)
            activity_runner = self._query(response.name, response.version)
            if activity_runner is None:
                logging.warning("No activity registered for: %s %s",
                                response.name, response.version)
                continue
            try:
                result = activity_runner.call(response.input, response)
            except Exception as e:
                response.terminate(e.message)
            else:
                response.complete(result)

    def __call__(self, name, version, task_list, *args, **kwargs):
        version = str(version)
        optional_args = [
            'heartbeat',
            'schedule_to_close',
            'schedule_to_start',
            'start_to_close',
        ]
        r_kwargs = {}
        for arg_name in optional_args:
            arg_value = kwargs.pop(arg_name, None)
            if arg_value is not None:
                r_kwargs[arg_name] = arg_value

        def wrapper(activity):
            r_kwargs['doc'] = activity.__doc__.strip()
            self.register(
                name, version, task_list, activity(*args, **kwargs), **r_kwargs
            )
            return activity

        return wrapper

    def _query(self, name, version):
        return self._activities.get((name, version))


workflow_client = WorkflowClient()
activity_client = ActivityClient()


def _str_or_none(maybe_none):
    if maybe_none is not None:
        return str(maybe_none)
