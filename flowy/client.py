import sys
import json
import uuid
import logging

from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions
from boto.swf.exceptions import SWFTypeAlreadyExistsError, SWFResponseError

from flowy.workflow import _UnhandledActivityError


__all__ = ['ActivityClient', 'WorkflowClient']


class SWFClient(object):
    """ A simple wrapper around Boto's SWF Layer1 that provides a cleaner
    interface and some convenience.

    Initialize and bind the client to a *domain* and a *task_list*.  A custom
    :py:class:`boto.swf.layer1.Layer1` instance can be sent as the *client*
    argument and it will be used instead of the default one.

    """
    def __init__(self, domain, task_list, client=None):
        self.client = client if client is not None else Layer1()
        self.domain = domain
        self.task_list = task_list
        self._scheduled_activities = []

    def register_workflow(self, name, version, workflow_runner,
                          execution_start_to_close=3600,
                          task_start_to_close=60,
                          child_policy='TERMINATE',
                          doc=None):
        """ Register a workflow with the given configuration options.

        If a workflow with the same *name* and *version* is already registered,
        this method returns a boolean indicating whether the registered
        workflow is compatible. A compatible workflow is a workflow that was
        registered using the same default values. The default total workflow
        running time can be specified in seconds using
        *execution_start_to_close* and a specific decision task runtime can be
        limited by setting *task_start_to_close*.

        """
        version = str(version)
        execution_start_to_close = str(execution_start_to_close)
        task_start_to_close = str(task_start_to_close)
        try:
            self.client.register_workflow_type(self.domain, name, version,
                                               self.task_list, child_policy,
                                               execution_start_to_close,
                                               task_start_to_close, doc)
            logging.info("Registered workflow: %s %s", name, version)
        except SWFTypeAlreadyExistsError:
            logging.warning("Workflow already registered: %s %s",
                            name, version)
            reg_w = self.client.describe_workflow_type(self.domain, name,
                                                       version)
            conf = reg_w['configuration']
            reg_estc = conf['defaultExecutionStartToCloseTimeout']
            reg_tstc = conf['defaultTaskStartToCloseTimeout']
            reg_tl = conf['defaultTaskList']['name']
            reg_cp = conf['defaultChildPolicy']

            if (reg_estc != execution_start_to_close
                    or reg_tstc != task_start_to_close
                    or reg_tl != self.task_list
                    or reg_cp != child_policy):
                logging.critical("Registered workflow "
                                 "has different defaults: %s %s",
                                 name, version)
                return False
        except SWFResponseError:
            logging.warning("Could not register workflow: %s %s",
                            name, version, exc_info=1)
            return False
        return True

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None,
                       schedule_to_close=None,
                       schedule_to_start=None,
                       start_to_close=None,
                       task_list=None):
        """ Queue an activity.

        This will schedule a run of a previously registered activity with the
        specified *name* and *version*. The *call_id* is used to assign a
        custom identity to this particular queued activity run inside its own
        workflow history.
        The queueing is done internally, without having the client make any
        requests yet. The actual scheduling is done by calling
        :meth:`SWFClient.schedule_activities`.

        The activity options specified here, if any, have a higher priority
        than the ones used when the activity was registered.
        For more information about the various arguments see
        :meth:`SWFClient.register_activity`.

        """
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
        """ Schedules all queued activities.

        All activities previously queued by :meth:`SWFClient.queue_activity`
        will be scheduled within the workflow identified by *token*.
        An optional textual *context* can be set and will be available in the
        workflow history.
        Returns a boolean indicating the success of the operation. On success
        the internal collection of scheduled activities will be cleared.
        """
        d = Layer1Decisions()
        for args, kwargs in self._scheduled_activities:
            d.schedule_activity_task(*args, **kwargs)
            name, version = args[1:]
            logging.info("Scheduled activity: %s %s", name, version)
        data = d._data
        try:
            self.client.respond_decision_task_completed(token, data, context)
            self._scheduled_activities = []
        except SWFResponseError:
            logging.warning("Could not send decisions: %s", token, exc_info=1)
            return False
        return True

    def complete_workflow(self, token, result):
        """ Signals the successful completion of the workflow.

        Completes the workflow identified by *token* with the *result* value.
        Returns a boolean indicating the success of the operation.

        """
        d = Layer1Decisions()
        d.complete_workflow_execution(result=result)
        data = d._data
        try:
            self.client.respond_decision_task_completed(token, decisions=data)
            logging.info("Completed workflow: %s %s", token, result)
        except SWFResponseError:
            logging.warning("Could not complete workflow: %s",
                            token, exc_info=1)
            return False
        return True

    def terminate_workflow(self, run_id, reason):
        """ Signals the termination of the workflow.

        Terminate the workflow identified by *run_id* for the specified
        *reason*. All the workflow activities will be abandoned and the final
        result won't be available.
        The *run_id* required here is the one obtained when
        :meth:`SWFClient.start_workflow` was called.  Returns a boolean
        indicating the success of the operation.

        """
        try:
            self.client.terminate_workflow_execution(self.domain, run_id,
                                                     reason=reason)
            logging.info("Terminated workflow: %s %s", run_id, reason)
        except SWFResponseError:
            logging.warning("Could not terminate workflow: %s",
                            run_id, exc_info=1)
            return False
        return True

    def poll_decision(self, next_page_token=None):
        """ Poll for a new decision task.

        Blocks until a decision is available in the task list bound to this
        client. In case of larger responses *next_page_token* can be used to
        retrieve paginated decisions.

        """
        poll = self.client.poll_for_decision_task
        while 1:
            try:
                return poll(self.domain, self.task_list, reverse_order=True,
                            next_page_token=next_page_token)
            except (IOError, SWFResponseError):
                logging.warning("Unknown error when pulling decisions: %s %s",
                                self.domain, self.task_list, exc_info=1)

    def next_decision(self):
        """ Get the next available decision.

        Returns the next :class:`flowy.client.Decision` instance available in
        the task list bound to this client. Because instantiating a
        ``Decision`` blocks until a decision is available, the same is true for
        this method.

        """
        return Decision(self)

    def register_activity(self, name, version, activity_runner,
                          heartbeat=60,
                          schedule_to_close=420,
                          schedule_to_start=120,
                          start_to_close=300,
                          doc=None):
        """ Register an activity with the given configuration options.

        If an activity with the same *name* and *version* is already
        registered, this method returns a boolean indicating whether the
        registered activity is compatible. A compatible activity is an
        activity that was registered using the same default values.
        The allowed running time can be specified in seconds using
        *start_to_close*, the allowed time from the moment it was scheduled,
        to the moment it finished can be specified using *schedule_to_close*
        and the time it can spend in the queue before the processing itself
        starts can be specified using *schedule_to_start*.

        """
        schedule_to_close = str(schedule_to_close)
        schedule_to_start = str(schedule_to_start)
        start_to_close = str(start_to_close)
        heartbeat = str(heartbeat)
        try:
            self.client.register_activity_type(self.domain, name, version,
                                               self.task_list, heartbeat,
                                               schedule_to_close,
                                               schedule_to_start,
                                               start_to_close, doc)
            logging.info("Registered activity: %s %s", name, version)
        except SWFTypeAlreadyExistsError:
            logging.warning("Activity already registered: %s %s",
                            name, version)
            reg_a = self.client.describe_activity_type(self.domain, name,
                                                       version)
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
                    or reg_tl != self.task_list):
                logging.critical("Registered activity "
                                 "has different defaults: %s %s",
                                 name, version)
                return False
        except SWFResponseError:
            logging.warning("Could not register activity: %s %s",
                            name, version, exc_info=1)
            return False
        return True

    def poll_activity(self):
        """ Poll for a new activity task.

        Blocks until an activity is available in the task list bound to this
        client.

        """
        poll = self.client.poll_for_activity_task
        while 1:
            try:
                return poll(self.domain, self.task_list)
            except (IOError, SWFResponseError):
                logging.warning("Unknown error when pulling activities: %s %s",
                                self.domain, self.task_list, exc_info=1)

    def complete_activity(self, token, result):
        """ Signals the successful completion of an activity.

        Completes the activity identified by *token* with the *result* value.
        Returns a boolean indicating the success of the operation.

        """
        try:
            self.client.respond_activity_task_completed(token, result)
            logging.info("Completed activity: %s %s", token, result)
        except SWFResponseError:
            logging.warning("Could not complete activity: %s",
                            token, exc_info=1)
            return False
        return True

    def terminate_activity(self, token, reason):
        """ Signals the termination of the activity.

        Terminate the activity identified by *token* for the specified
        *reason*. Returns a boolean indicating the success of the operation.
        """
        try:
            self.client.respond_activity_task_failed(token, reason=reason)
            logging.info("Terminated activity: %s %s", token, reason)
        except SWFResponseError:
            logging.warning("Could not terminate activity: %s",
                            token, exc_info=1)
            return False
        return True

    def heartbeat(self, token):
        """ Report that the activity identified by *token* is still making
        progress.

        Returns a boolean indicating the success of the operation or whether
        the heartbeat exceeded the time it should have taken to report activity
        progress. In the latter case the activity execution should be stopped.

        """
        try:
            self.client.record_activity_task_heartbeat(token)
            logging.info("Sent activity heartbeat: %s", token)
        except SWFResponseError:
            logging.warning("Error when sending activity heartbeat: %s",
                            token, exc_info=1)
            return False
        return True

    def next_activity(self):
        """ Get the next available activity.

        Returns the next :class:`flowy.client.ActivityResponse` instance
        available in the task list bound to this client. Because
        instantiating an ``ActivityResponse`` blocks until an activity is
        available, the same is true for this method.

        """
        return ActivityResponse(self)

    def start_workflow(self, name, version, input):
        """ Starts the workflow identified by *name* and *version* with the
        given *input*.

        Returns the ``run_id`` that can be used to uniquely identify the
        workflow execution within a domain. If starting the execution
        encounters an error, ``None`` is returned.  The returned ``run_id`` can
        be used when calling :meth:`flowy.client.SWFClient.terminate_workflow`.

        """
        try:
            r = self.client.start_workflow_execution(self.domain,
                                                     str(uuid.uuid4()),
                                                     name, str(version),
                                                     task_list=self.task_list,
                                                     input=input)
        except SWFResponseError:
            logging.warning("Could not start workflow: %s %s",
                            name, version, exc_info=1)
            return None
        return r['runId']


class Decision(object):
    """ An object that works with the workflow execution history through the
    bound *client*.

    Initializing this class will block the current execution thread until
    a decision will be polled. It is also responsible for managing the
    execution context.

    """
    def __init__(self, client):
        self.client = client
        self._event_to_call_id = {}
        self._retries = {}
        self._scheduled = set()
        self._results = {}
        self._timed_out = set()
        self._with_errors = {}
        self.input = None

        response = {}
        while 'taskToken' not in response or not response['taskToken']:
            response = self.client.poll_decision()
        self._api_response = response

        self._restore_context()

        # Update the context with all new events
        for new_event in self._new_events:
            event_type = new_event['eventType']
            getattr(self, '_%s' % event_type, lambda x: 0)(new_event)

        # Assuming workflow started is always the first event
        assert self.input is not None

    @property
    def name(self):
        """ The name of the workflow type defined together with
        :meth:`flowy.client.Decision.version`.

        """
        return self._api_response['workflowType']['name']

    @property
    def version(self):
        """ The version of the workflow type defined together with
        :meth:`flowy.client.Decision.name`.

        """
        return self._api_response['workflowType']['version']

    def queue_activity(self, call_id, name, version, input,
                       heartbeat=None,
                       schedule_to_close=None,
                       schedule_to_start=None,
                       start_to_close=None,
                       task_list=None,
                       retries=3):
        """ Queue an activity using the bound client's
        :meth:`flowy.client.SWFClient.queue_activity` method.

        This method also initializes the internal retry counter with a default
        value or the one specified with *retries*. The aforementioned attribute
        is used in conjunction with :meth:`flowy.client.Decision.should_retry`
        in order to determine whether an activity should be rescheduled.
        The total number of runs an activity will perform is the initial
        run plus the number of retries. Whenever an activity times out, the
        number of retries associated with that activity is decremented by
        1, until it reaches 0.

        """
        self.client.queue_activity(
            call_id, name, version, input,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_list=task_list
        )
        self._retries.setdefault(call_id, retries + 1)

    def schedule_activities(self):
        """ Schedule all queued activities using the bound client's
        :meth:`flowy.client.SWFClient.schedule_activities` method.

        This method is also responsible for passing the ``token`` that
        identifies the workflow the activities will be scheduled within.

        """
        self.client.schedule_activities(self._token, self._serialize_context())

    def complete_workflow(self, result):
        """ Signal the successful completion of the workflow with a given
        *result* using the bound client's
        :meth:`flowy.client.SWFClient.complete_workflow` method.

        This method is also responsable for passing the ``token`` that
        identifies the workflow that successfully completed.

        """
        return self.client.complete_workflow(self._token, result)

    def terminate_workflow(self, reason):
        """ Signal the termination of the workflow with a given *reason* using
        the bound client's :meth:`flowy.client.SWFClient.terminate_workflow`
        method.

        This method is also responsable for passing the ``run_id`` that
        identifies the workflow that terminated.

        """
        run_id = self._api_response['workflowExecution']['workflowId']
        return self.client.terminate_workflow(run_id, reason)

    def any_activity_running(self):
        """ Checks whether there are any activities running.

        Any activities that have been scheduled but not yet completed are
        considered to be running.  Returns a boolean indicating if there are
        any activities with the aforementioned property.

        """
        return bool(self._scheduled)

    def is_activity_scheduled(self, call_id):
        """ Checks whether the activity with *call_id* is scheduled. """
        return call_id in self._scheduled

    def activity_result(self, call_id, default=None):
        """ Return the result for the activity identified by *call_id*
        with an optional *default* value.

        The *call_id* is the internal activity identifier generated by the
        :class:`flowy.workflow.Workflow` object and should not be confused with
        ``event_id``.
        """
        return self._results.get(call_id, default)

    def activity_error(self, call_id, default=None):
        """ Return the reason why the activity identified by *call_id* failed
        or a *default* one if no such reason is found.

        """
        return self._with_errors.get(call_id, default)

    def is_activity_timedout(self, call_id):
        """ Check whether the activity identified by *call_id* timed out. """
        return call_id in self._timed_out

    def should_retry(self, call_id):
        """ Check whether the activity identified by *call_id* should be
        retried.

        When the retry counter of an activity reaches 0, is no longer eligible
        for any retries.
        """
        return self._retries[call_id] > 0

    def _ActivityTaskScheduled(self, event):
        event_id = event['eventId']
        subdict = event['activityTaskScheduledEventAttributes']
        call_id = int(subdict['activityId'])
        self._event_to_call_id[event_id] = call_id
        self._scheduled.add(call_id)
        if call_id in self._timed_out:
            self._timed_out.remove(call_id)

    def _ActivityTaskCompleted(self, event):
        subdict = event['activityTaskCompletedEventAttributes']
        event_id, result = subdict['scheduledEventId'], subdict['result']
        self._scheduled.remove(self._event_to_call_id[event_id])
        self._results[self._event_to_call_id[event_id]] = result

    def _ActivityTaskFailed(self, event):
        subdict = event['activityTaskFailedEventAttributes']
        event_id, reason = subdict['scheduledEventId'], subdict['reason']
        self._with_errors[self._event_to_call_id[event_id]] = reason

    def _ActivityTaskTimedOut(self, event):
        subdict = event['activityTaskTimedOutEventAttributes']
        event_id = subdict['scheduledEventId']
        self._scheduled.remove(self._event_to_call_id[event_id])
        self._timed_out.add(self._event_to_call_id[event_id])
        self._retries[self._event_to_call_id[event_id]] -= 1

    def _WorkflowExecutionStarted(self, event):
        subdict = event['workflowExecutionStartedEventAttributes']
        self.input = subdict['input']

    def _restore_context(self):
        if self._context is not None:
            try:
                initial_state = json.loads(self._context)
                self._event_to_call_id = self._fix_keys(
                    initial_state['event_to_call_id']
                )
                self._retries = self._fix_keys(initial_state['retries'])
                self._scheduled = set(initial_state['scheduled'])
                self._results = self._fix_keys(initial_state['results'])
                self._timed_out = set(initial_state['timed_out'])
                self._with_errors = self._fix_keys(
                    initial_state['with_errors']
                )
                self.input = initial_state['input']
            except (ValueError, KeyError):
                logging.critical("Could not load context: %s" % self._context)
                exit(1)

    def _serialize_context(self):
        return json.dumps({
            'event_to_call_id': self._event_to_call_id,
            'retries': self._retries,
            'scheduled': list(self._scheduled),
            'results': self._results,
            'timed_out': list(self._timed_out),
            'with_errors': self._with_errors,
            'input': self.input,
        })

    @property
    def _token(self):
        return self._api_response['taskToken']

    @property
    def _context(self):
        for event in self._new_events:
            if event['eventType'] == 'DecisionTaskCompleted':
                DTCEA = 'decisionTaskCompletedEventAttributes'
                return event[DTCEA]['executionContext']
        return None

    @property
    def _events(self):
        if not hasattr(self, '_cached_events'):
            events = []
            api_response = self._api_response
            while api_response.get('nextPageToken'):
                for event in api_response['events']:
                    events.append(event)
                api_response = self.client.poll_decision(
                    next_page_token=api_response['nextPageToken']
                )
            for event in api_response['events']:
                events.append(event)
            self._cached_events = events
        return self._cached_events

    @property
    def _new_events(self):
        decisions_completed = 0
        events = []
        prev_id = self._api_response.get('previousStartedEventId')
        for event in self._events:
            if event['eventType'] == 'DecisionTaskCompleted':
                decisions_completed += 1
            if prev_id and event['eventId'] == prev_id:
                break
            events.append(event)
        assert decisions_completed <= 1
        return reversed(events)

    @staticmethod
    def _fix_keys(d):
        # Fix json's stupid silent key conversion from int to string
        return dict((int(key), value) for key, value in d.items())


class WorkflowLoop(object):
    def __init__(self, client):
        self.client = client
        self.workflows = {}

    def register(self, name, version, workflow_runner,
                 execution_start_to_close=3600,
                 task_start_to_close=60,
                 child_policy='TERMINATE',
                 doc=None):
        self.workflows[(name, str(version))] = workflow_runner
        return self.client.register_workflow(
            name,
            version,
            workflow_runner,
            execution_start_to_close,
            task_start_to_close,
            child_policy,
            doc
        )

    def start(self):
        while 1:
            decision = self.client.next_decision()
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

    def _query(self, name, version):
        return self.workflows.get((name, version))


class WorkflowClient(object):
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client

    @classmethod
    def for_domain(cls, domain, task_list):
        client = SWFClient(domain, task_list)
        loop = WorkflowLoop(client)
        return cls(loop, client)

    @classmethod
    def from_client(cls, client):
        loop = WorkflowLoop(client)
        return cls(loop, client)

    def __call__(self, name, version, *args, **kwargs):
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
            if not self.loop.register(name, version, workflow(*args, **kwargs),
                                      **r_kwargs):
                sys.exit(1)
            return workflow

        return wrapper

    def schedule(self, name, version, *args, **kwargs):
        input = self.serialize_workflow_arguments(*args, **kwargs)
        return self.client.start_workflow(name, version, input)

    @staticmethod
    def serialize_workflow_arguments(*args, **kwargs):
        return json.dumps({"args": args, "kwargs": kwargs})

    def start(self):
        logging.info("Starting workflow client loop...")
        self.loop.start()


class ActivityResponse(object):
    def __init__(self, client):
        self.client = client
        response = self.client.poll_activity()
        while 'taskToken' not in response or not response['taskToken']:
            response = self.client.poll_activity()
        self._api_response = response

    def complete(self, result):
        return self.client.complete_activity(self._token, result)

    def terminate(self, reason):
        return self.client.terminate_activity(self._token, reason)

    def heartbeat(self):
        return self.client.heartbeat(self._token)

    @property
    def name(self):
        return self._api_response['activityType']['name']

    @property
    def version(self):
        return self._api_response['activityType']['version']

    @property
    def input(self):
        return self._api_response['input']

    @property
    def _token(self):
        return self._api_response['taskToken']


class ActivityLoop(object):
    def __init__(self, client):
        self.client = client
        self.activities = {}

    def register(self, name, version, activity_runner,
                 heartbeat=60,
                 schedule_to_close=420,
                 schedule_to_start=120,
                 start_to_close=300,
                 doc=None):
        # All versions are converted to string in SWF and that's how we should
        # store them too in order to be able to query for them
        self.activities[(name, str(version))] = activity_runner
        return self.client.register_activity(
            name, version, activity_runner, heartbeat,
            schedule_to_close, schedule_to_start, start_to_close, doc
        )

    def start(self):
        while 1:
            response = self.client.request_activity()
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

    def _query(self, name, version):
        # XXX: if we can't resolve this activity log the error and continue
        return self.activities.get((name, version))


class ActivityClient(object):
    def __init__(self, loop):
        self.loop = loop

    @classmethod
    def for_domain(cls, domain, task_list):
        client = SWFClient(domain, task_list)
        loop = ActivityLoop(client)
        return cls(loop)

    @classmethod
    def from_client(cls, client):
        loop = ActivityLoop(client)
        return cls(loop)

    def __call__(self, name, version, *args, **kwargs):
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
            if not self.loop.register(name, version, activity(*args, **kwargs),
                                      **r_kwargs):
                sys.exit(1)
            return activity

        return wrapper

    def start(self):
        logging.info("Starting activity client loop...")
        self.loop.start()


def _str_or_none(maybe_none):
    if maybe_none is not None:
        return str(maybe_none)
