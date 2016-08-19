import os
import socket

import venusian
from botocore.exceptions import ClientError

from flowy.swf.client import SWFClient, IDENTITY_SIZE
from flowy.swf.decision import SWFActivityDecision
from flowy.swf.decision import SWFWorkflowDecision
from flowy.swf.history import SWFExecutionHistory
from flowy.utils import logger
from flowy.utils import setup_default_logger
from flowy.worker import Worker


__all__ = ['SWFWorkflowWorker', 'SWFActivityWorker']


class SWFWorker(Worker):
    def __init__(self):
        super(SWFWorker, self).__init__()
        self.remote_reg_callbacks = []

    def __call__(self, name, version, input_data, decision, *extra_args):
        return super(SWFWorker, self).__call__(
            (str(name), str(version)), input_data, decision, *extra_args)

    def register_remote(self, swf_client, domain):
        """Register or check compatibility of all configs in Amazon SWF."""
        for remote_reg_callback in self.remote_reg_callbacks:
            # Raises if there are registration problems
            remote_reg_callback(swf_client, domain)

    def register(self, config, func, version, name=None):
        super(SWFWorker, self).register(config, func, (name, version))

    def add_remote_reg_callback(self, callback):
        self.remote_reg_callbacks.append(callback)

    def make_scanner(self):
        return venusian.Scanner(
            register_task=self.register_task,
            add_remote_reg_callback=self.add_remote_reg_callback)


class SWFWorkflowWorker(SWFWorker):
    categories = ['swf_workflow']

    # Be explicit about what arguments are expected
    def __call__(self, name, version, input_data, decision, execution_history):
        super(SWFWorkflowWorker, self).__call__(
            name, version, input_data, decision,    # needed for worker logic
            decision, execution_history)    # extra_args passed to proxies

    def break_loop(self):
        """Used to exit the loop in tests. Return True to break."""
        return False

    def run_forever(self, domain, task_list,
                    swf_client=None,
                    setup_log=True,
                    register_remote=True,
                    identity=None):
        """Starts an endless single threaded/single process worker loop.

        The worker polls endlessly for new decisions from the specified domain
        and task list and runs them.

        If reg_remote is set, all registered workflow are registered remotely.

        An identity can be set to track this worker in the SWF console,
        otherwise a default identity is generated from this machine domain and
        process pid.

        If setup_log is set, a default configuration for the logger is loaded.

        A custom SWF client can be passed in swf_client, otherwise a default
        client is used.
        """
        if setup_log:
            setup_default_logger()
        identity = default_identity() if identity is None else identity
        swf_client = SWFClient() if swf_client is None else swf_client
        if register_remote:
            self.register_remote(swf_client, domain)
        try:
            while 1:
                if self.break_loop():
                    break
                name, version, input_data, exec_history, decision = poll_decision(
                    swf_client, domain, task_list, identity)
                self(name, version, input_data, decision, exec_history)
        except KeyboardInterrupt:
            pass


class SWFActivityWorker(SWFWorker):
    categories = ['swf_activity']

    # Be explicit about what arguments are expected
    def __call__(self, name, version, input_data, decision):
        # No extra arguments are used
        super(SWFActivityWorker, self).__call__(
            name, version, input_data, decision,    # needed for worker logic
            decision.heartbeat)     # extra_args

    def break_loop(self):
        """Used to exit the loop in tests. Return True to break."""
        return False

    def run_forever(self, domain, task_list,
                    swf_client=None,
                    setup_log=True,
                    register_remote=True,
                    identity=None):
        """Same as SWFWorkflowWorker.run_forever but for activities."""
        if setup_log:
            setup_default_logger()
        identity = default_identity() if identity is None else identity
        swf_client = SWFClient() if swf_client is None else swf_client
        if register_remote:
            self.register_remote(swf_client, domain)
        try:
            while 1:
                if self.break_loop():
                    break
                swf_response = {}
                while not swf_response.get('taskToken'):
                    try:
                        swf_response = swf_client.poll_for_activity_task(
                            domain, task_list, identity=identity)
                    except ClientError:
                        # add a delay before retrying?
                        logger.exception('Error while polling for activities:')

                at = swf_response['activityType']
                decision = SWFActivityDecision(swf_client, swf_response['taskToken'])
                self(at['name'], at['version'], swf_response['input'], decision)
        except KeyboardInterrupt:
            pass


def default_identity():
    """Generate a local identity string for this process."""
    identity = "%s-%s" % (socket.getfqdn(), os.getpid())
    return identity[-IDENTITY_SIZE:]    # keep the most important part


def poll_decision(swf_client, domain, task_list, identity=None):
    """Poll a decision and create a SWFWorkflowContext structure.

    :type swf_client: :class:`SWFClient`
    :param swf_client: an implementation or duck typing of :class:`SWFClient`
    :param domain: the domain containing the task list to poll
    :param task_list: the task list from which to poll decision
    :param identity: an identity str of the request maker

    :rtype: tuple
    :returns: a tuple consisting of (name, version, input_data,
        :class:'SWFExecutionHistory', :class:`SWFWorkflowDecision`)
    """
    first_page = poll_first_page(swf_client, domain, task_list, identity)
    token = first_page['taskToken']
    all_events = events(swf_client, domain, task_list, first_page, identity)
    # Sometimes the first event is on the second page,
    # and the first page is empty
    first_event = next(all_events)
    assert first_event['eventType'] == 'WorkflowExecutionStarted'
    wesea = 'workflowExecutionStartedEventAttributes'
    assert first_event[wesea]['taskList']['name'] == task_list
    task_duration = first_event[wesea]['taskStartToCloseTimeout']
    workflow_duration = first_event[wesea]['executionStartToCloseTimeout']
    tags = first_event[wesea].get('tagList', None)
    child_policy = first_event[wesea]['childPolicy']
    name = first_event[wesea]['workflowType']['name']
    version = first_event[wesea]['workflowType']['version']
    input_data = first_event[wesea]['input']
    try:
        running, timedout, results, errors, order = load_events(all_events)
    except _PaginationError:
        # There's nothing better to do than to retry
        return poll_decision(swf_client, domain, task_list, identity)
    execution_history = SWFExecutionHistory(running, timedout, results, errors, order)
    decision = SWFWorkflowDecision(swf_client, token, name, version, task_list,
                                   task_duration, workflow_duration, tags,
                                   child_policy)
    return name, version, input_data, execution_history, decision


def poll_first_page(swf_client, domain, task_list, identity=None):
    """Return the response from loading the first page. In case of errors,
    empty responses or whatnot retry until a valid response.

    :type swf_client: :class:`SWFClient`
    :param swf_client: an implementation or duck typing of :class:`SWFClient`
    :param domain: the domain containing the task list to poll
    :param task_list: the task list from which to poll for events
    :param identity: an identity str of the request maker

    :rtype: dict[str, str|int|list|dict]
    :returns: a dict containing workflow information and list of events
    """
    swf_response = {}
    while not swf_response.get('taskToken'):
        try:
            swf_response = swf_client.poll_for_decision_task(domain, task_list,
                                                             identity=identity)
        except ClientError:
            logger.exception('Error while polling for decisions:')
    return swf_response


def poll_page(swf_client, domain, task_list, token, identity=None):
    """Return a specific page. In case of errors retry a number of times.

    :type swf_client: :class:`SWFClient`
    :param swf_client: an implementation or duck typing of :class:`SWFClient`
    :param domain: the domain containing the task list to poll
    :param task_list: the task list from which to poll for events
    :param token: the token string for the requested page
    :param identity: an identity str of the request maker

    :rtype: dict[str, str|int|list|dict]
    :returns: a dict containing workflow information and list of events
    """
    for _ in range(7):  # give up after a limited number of retries
        try:
            swf_response = swf_client.poll_for_decision_task(
                domain, task_list, identity=identity, next_page_token=token)
            break
        except ClientError:
            logger.exception('Error while polling for decision page:')
    else:
        raise _PaginationError()
    return swf_response


def events(swf_client, domain, task_list, first_page, identity=None):
    """Load pages one by one and generate all events found.

    :type swf_client: :class:`SWFClient`
    :param swf_client: an implementation or duck typing of :class:`SWFClient`
    :param domain: the domain containing the task list to poll
    :param task_list: the task list from which to poll for events
    :param first_page: the page dict structure from which to start generating
        the events, usually the response from :func:`poll_first_page`
    :param identity: an identity str of the request maker

    :rtype: collections.Iterator[dict[str, int|str|dict[str, int|str|dict]]
    :returns: iterator over all of the events
    """
    page = first_page
    while 1:
        for event in page['events']:
            yield event
        if not page.get('nextPageToken'):
            break
        page = poll_page(swf_client, domain, task_list, page['nextPageToken'],
                         identity=identity)


def load_events(event_iter):
    """Combine all events in their order.

    This returns a tuple of the following things:
        running  - a set of the ids of running tasks
        timedout - a set of the ids of tasks that have timedout
        results  - a dictionary of id -> result for each finished task
        errors   - a dictionary of id -> error message for each failed task
        order    - an list of task ids in the order they finished
    """
    running, timedout = set(), set()
    results, errors = {}, {}
    order = []
    event2call = {}
    for event in event_iter:
        e_type = event.get('eventType')
        if e_type == 'ActivityTaskScheduled':
            eid = event['activityTaskScheduledEventAttributes']['activityId']
            event2call[event['eventId']] = eid
            running.add(eid)
        elif e_type == 'ActivityTaskCompleted':
            atcea = 'activityTaskCompletedEventAttributes'
            eid = event2call[event[atcea]['scheduledEventId']]
            result = event[atcea]['result']
            running.remove(eid)
            results[eid] = result
            order.append(eid)
        elif e_type == 'ActivityTaskFailed':
            atfea = 'activityTaskFailedEventAttributes'
            eid = event2call[event[atfea]['scheduledEventId']]
            reason = event[atfea]['reason']
            running.remove(eid)
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'ActivityTaskTimedOut':
            attoea = 'activityTaskTimedOutEventAttributes'
            eid = event2call[event[attoea]['scheduledEventId']]
            running.remove(eid)
            timedout.add(eid)
            order.append(eid)
        elif e_type == 'ScheduleActivityTaskFailed':
            satfea = 'scheduleActivityTaskFailedEventAttributes'
            eid = event[satfea]['activityId']
            reason = event[satfea]['cause']
            # when a job is not found it's not even started
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'StartChildWorkflowExecutionInitiated':
            scweiea = 'startChildWorkflowExecutionInitiatedEventAttributes'
            eid = _subworkflow_call_key(event[scweiea]['workflowId'])
            running.add(eid)
        elif e_type == 'ChildWorkflowExecutionCompleted':
            cwecea = 'childWorkflowExecutionCompletedEventAttributes'
            eid = _subworkflow_call_key(
                event[cwecea]['workflowExecution']['workflowId'])
            result = event[cwecea]['result']
            running.remove(eid)
            results[eid] = result
            order.append(eid)
        elif e_type == 'ChildWorkflowExecutionFailed':
            cwefea = 'childWorkflowExecutionFailedEventAttributes'
            eid = _subworkflow_call_key(
                event[cwefea]['workflowExecution']['workflowId'])
            reason = event[cwefea]['reason']
            running.remove(eid)
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'ChildWorkflowExecutionTimedOut':
            cwetoea = 'childWorkflowExecutionTimedOutEventAttributes'
            eid = _subworkflow_call_key(
                event[cwetoea]['workflowExecution']['workflowId'])
            running.remove(eid)
            timedout.add(eid)
            order.append(eid)
        elif e_type == 'StartChildWorkflowExecutionFailed':
            scwefea = 'startChildWorkflowExecutionFailedEventAttributes'
            eid = _subworkflow_call_key(event[scwefea]['workflowId'])
            reason = event[scwefea]['cause']
            errors[eid] = reason
            order.append(eid)
        elif e_type == 'TimerStarted':
            eid = event['timerStartedEventAttributes']['timerId']
            running.add(eid)
        elif e_type == 'TimerFired':
            eid = event['timerFiredEventAttributes']['timerId']
            running.remove(eid)
            results[eid] = None
    return running, timedout, results, errors, order


class _PaginationError(Exception):
    """Can't retrieve the next page after X retries."""


def _subworkflow_call_key(w_id):
    return w_id.split(':')[-1]
