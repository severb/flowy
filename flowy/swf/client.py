import boto3
from botocore.client import Config

from flowy.utils import str_or_none

__all__ = ['CHILD_POLICY', 'DURATION', 'IDENTITY_SIZE', 'SWFClient']


# poor man's enums and constants
class CHILD_POLICY:
    TERMINATE = 'TERMINATE'
    REQUEST_CANCEL = 'REQUEST_CANCEL'
    ABANDON = 'ABANDON'

    ALL = ('TERMINATE', 'REQUEST_CANCEL', 'ABANDON')


class DURATION:
    INDEF = 'NONE'
    ONE_YEAR = 31622400  # seconds in a leap year; 60 * 60 * 24 * 366

    ALL = ('NONE', 31622400)


IDENTITY_SIZE = 256


class SWFClient(object):
    """A thin wrapper around :func:`boto3.client('swf')` for sanitizing
    parameters and maybe error handling. This will be interfacing in the
    :mod:`flowy.swf` for communicating to AWS SWF. Custom clients may be used,
    interfacing this class.
    """

    def __init__(self, client=None, config=None, kwargs=None):
        """Setup initial swf client. Can inject an initialized SWF client,
        ignoring the additional config or config can be passed to create the
        SWF client.

        Additional keyword arguments can be passed in a dict to initialise the
        low level client. The key 'config' takes precedence over the `config`
        kwarg. See :py:meth:`boto3.session.Session.client`.

        :type client: botocore.client.BaseClient
        :param client: a low-level service client for swf.
            See :py:meth:`boto3.session.Session.client`

        :type config: botocore.client.Config
        :param config: custom config to instantiate the 'swf' client; by default
            it sets connection and read timeouts to 70 sec

        :type kwargs: dict
        :param kwargs: kwargs for passing to client initialisation. The config
            param can be overwritten here
        """
        kwargs = kwargs if isinstance(kwargs, dict) else {}
        config = config or Config(connect_timeout=70, read_timeout=70)
        kwargs.setdefault('config', config)

        self.client = client or boto3.client('swf', **kwargs)

    def register_activity_type(self, domain, name, version, desc=None,
                               default_task_list=None,
                               default_priority=None,
                               default_heartbeat_timeout=None,
                               default_exec_timeout=None,
                               default_start_timeout=None,
                               default_close_timeout=None):
        """Wrapper for `boto3.client('swf').register_activity_type`."""
        kwargs = {
            'domain': str_or_none(domain),
            'name': str_or_none(name),
            'version': str_or_none(version),
            'description': str_or_none(desc),
            'defaultTaskList': {
                'name': str_or_none(default_task_list)
            },
            'defaultTaskPriority': str_or_none(default_priority),
            'defaultTaskHeartbeatTimeout': duration_encode(default_heartbeat_timeout,
                                                           'default_heartbeat_timeout'),
            'defaultTaskStartToCloseTimeout': duration_encode(default_exec_timeout,
                                                              'default_exec_timeout'),
            'defaultTaskScheduleToStartTimeout': duration_encode(default_start_timeout,
                                                                 'default_start_timeout'),
            'defaultTaskScheduleToCloseTimeout': duration_encode(default_close_timeout,
                                                                 'default_close_timeout')
        }
        normalize_data(kwargs)
        response = self.client.register_activity_type(**kwargs)
        return response

    def register_workflow_type(self, domain, name, version, desc=None,
                               default_task_list=None,
                               default_priority=None,
                               default_task_timeout=None,
                               default_exec_timeout=None,
                               default_child_policy=None,
                               default_lambda_role=None):
        """Wrapper for `boto3.client('swf').register_workflow_type`."""
        kwargs = {
            'domain': str_or_none(domain),
            'name': str_or_none(name),
            'version': str_or_none(version),
            'description': str_or_none(desc),
            'defaultTaskList': {
                'name': str_or_none(default_task_list)
            },
            'defaultTaskPriority': str_or_none(default_priority),
            'defaultTaskStartToCloseTimeout': duration_encode(default_task_timeout,
                                                              'default_task_timeout'),
            'defaultExecutionStartToCloseTimeout': duration_encode(default_exec_timeout,
                                                                   'default_exec_timeout'),
            'defaultChildPolicy': cp_encode(default_child_policy),
            'defaultLambdaRole': str_or_none(default_lambda_role)
        }
        normalize_data(kwargs)
        response = self.client.register_workflow_type(**kwargs)
        return response

    def describe_activity_type(self, domain, name, version):
        """Wrapper for `boto3.client('swf').describe_activity_type`."""
        kwargs = {
            'domain': str_or_none(domain),
            'activityType': {
                'name': str_or_none(name),
                'version': str_or_none(version)
            }
        }
        normalize_data(kwargs)
        response = self.client.describe_activity_type(**kwargs)
        return response

    def describe_workflow_type(self, domain, name, version):
        """Wrapper for `boto3.client('swf').describe_workflow_type`."""
        kwargs = {
            'domain': str_or_none(domain),
            'workflowType': {
                'name': str_or_none(name),
                'version': str_or_none(version)
            }
        }
        normalize_data(kwargs)
        response = self.client.describe_workflow_type(**kwargs)
        return response

    def start_workflow_execution(self, domain, wid, name, version,
                                 input=None, priority=None, task_list=None,
                                 execution_start_to_close_timeout=None,
                                 task_start_to_close_timeout=None,
                                 child_policy=None, tags=None,
                                 lambda_role=None):
        """Wrapper for `boto3.client('swf').start_workflow_execution`."""
        kwargs = {
            'domain': str_or_none(domain),
            'workflowId': str_or_none(wid),
            'workflowType': {
                'name': str_or_none(name),
                'version': str_or_none(version)
            },
            'input': str_or_none(input),
            'taskPriority': str_or_none(priority),
            'taskList': {
                'name': str_or_none(task_list)
            },
            'executionStartToCloseTimeout': str_or_none(execution_start_to_close_timeout),
            'taskStartToCloseTimeout': str_or_none(task_start_to_close_timeout),
            'childPolicy': cp_encode(child_policy),
            'tagList': tags_encode(tags),
            'lambda_role': str_or_none(lambda_role)
        }
        normalize_data(kwargs)
        response = self.client.start_workflow_execution(**kwargs)
        return response

    def poll_for_decision_task(self, domain, task_list, identity=None,
                               next_page_token=None, max_page_size=1000,
                               reverse_order=False):
        """Wrapper for `boto3.client('swf').poll_for_decision_task`."""
        assert max_page_size <= 1000, 'Page size greater than 1000.'
        identity = str(identity)[:IDENTITY_SIZE] if identity else identity

        kwargs = {
            'domain': str_or_none(domain),
            'taskList': {
                'name': str_or_none(task_list)
            },
            'identity': identity,
            'nextPageToken': str_or_none(next_page_token),
            'maximumPageSize': max_page_size,
            'reverseOrder': reverse_order
        }
        normalize_data(kwargs)
        response = self.client.poll_for_decision_task(**kwargs)
        return response

    def poll_for_activity_task(self, domain, task_list, identity=None):
        """Wrapper for `boto3.client('swf').poll_for_activity_task`."""
        identity = str(identity)[:IDENTITY_SIZE] if identity else identity

        kwargs = {
            'domain': str_or_none(domain),
            'taskList': {
                'name': str_or_none(task_list),
            },
            'identity': identity,
        }
        normalize_data(kwargs)
        response = self.client.poll_for_activity_task(**kwargs)
        return response

    def record_activity_task_heartbeat(self, task_token, details=None):
        """Wrapper for `boto3.client('swf').record_activity_task_heartbeat`."""
        kwargs = {
            'taskToken': str_or_none(task_token),
            'details': str_or_none(details),
        }
        normalize_data(kwargs)
        response = self.client.record_activity_task_heartbeat(**kwargs)
        return response

    def respond_activity_task_failed(self, task_token, reason=None, details=None):
        """Wrapper for `boto3.client('swf').respond_activity_task_failed`."""
        kwargs = {
            'taskToken': str_or_none(task_token),
            'reason': str_or_none(reason),
            'details': str_or_none(details)
        }
        normalize_data(kwargs)
        response = self.client.respond_activity_task_failed(**kwargs)
        return response

    def respond_activity_task_completed(self, task_token, result=None):
        """Wrapper for `boto3.client('swf').respond_activity_task_completed`."""
        kwargs = {
            'taskToken': str_or_none(task_token),
            'result': str_or_none(result)
        }
        normalize_data(kwargs)
        response = self.client.respond_activity_task_completed(**kwargs)
        return response

    def respond_decision_task_completed(self, task_token, decisions=None,
                                        exec_context=None):
        """Wrapper for `boto3.client('swf').respond_decision_task_completed`."""
        kwargs = {
            'taskToken': str_or_none(task_token),
            'decisions': decisions or [],
            'executionContext': str_or_none(exec_context)
        }
        normalize_data(kwargs)
        response = self.client.respond_decision_task_completed(**kwargs)
        return response


class SWFDecisions(object):
    """
    Helper class for creating decision responses.
    """
    def __init__(self):
        """
        Use this object to build a list of decisions for a decision response.
        Each method call will add append a new decision.  Retrieve the list
        of decisions from the _data attribute.
        """
        self._data = []

    def schedule_activity_task(self,
                               activity_id,
                               activity_type_name,
                               activity_type_version,
                               task_list=None,
                               task_priority=None,
                               control=None,
                               heartbeat_timeout=None,
                               schedule_to_close_timeout=None,
                               schedule_to_start_timeout=None,
                               start_to_close_timeout=None,
                               input=None):
        """
        Schedules an activity task.

        :type activity_id: string
        :param activity_id: The activityId of the type of the activity
            being scheduled.

        :type activity_type_name: string
        :param activity_type_name: The name of the type of the activity
            being scheduled.

        :type activity_type_version: string|int|float
        :param activity_type_version: The version of the type of the
            activity being scheduled.

        :type task_list: string
        :param task_list: If set, specifies the name of the task list in
            which to schedule the activity task. If not specified, the
            defaultTaskList registered with the activity type will be used.
            Note: a task list for this activity task must be specified either
            as a default for the activity type or through this field. If
            neither this field is set nor a default task list was specified
            at registration time then a fault will be returned.
        """
        d = {
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': {
                'activityId': activity_id,
                'activityType': {
                    'name': activity_type_name,
                    'version': str(activity_type_version)
                },
                'taskList': {'name': str_or_none(task_list)},
                'taskPriority': str_or_none(task_priority),
                'control': control,
                'heartbeatTimeout': duration_encode(heartbeat_timeout,
                                                    'heartbeat_timeout'),
                'scheduleToCloseTimeout': duration_encode(
                    schedule_to_close_timeout, 'schedule_to_close_timeout'),
                'scheduleToStartTimeout': duration_encode(
                    schedule_to_start_timeout, 'schedule_to_start_timeout'),
                'startToCloseTimeout': duration_encode(start_to_close_timeout,
                                                       'start_to_close_timeout'),
                'input': str_or_none(input)
            }
        }
        normalize_data(d)
        self._data.append(d)

    def request_cancel_activity_task(self, activity_id):
        """
        Attempts to cancel a previously scheduled activity task. If
        the activity task was scheduled but has not been assigned to a
        worker, then it will be canceled. If the activity task was
        already assigned to a worker, then the worker will be informed
        that cancellation has been requested in the response to
        RecordActivityTaskHeartbeat.

        :param str activity_id: The activityId of the type of the activity
            being canceled.
        """
        d = {
            'decisionType': 'RequestCancelActivityTask',
            'requestCancelActivityTaskDecisionAttributes': {
                'activityId': activity_id
            }
        }
        self._data.append(d)

    def record_marker(self, marker_name, details=None):
        """
        Records a MarkerRecorded event in the history. Markers can be
        used for adding custom information in the history for instance
        to let deciders know that they do not need to look at the
        history beyond the marker event.

        :param str marker_name: the name if the marker
        """
        d = {
            'decisionType': 'RecordMarker',
            'recordMarkerDecisionAttributes': {
                'markerName': marker_name,
                'details': str_or_none(details)
            }
        }
        normalize_data(d)
        self._data.append(d)

    def complete_workflow_execution(self, result=None):
        """
        Closes the workflow execution and records a WorkflowExecutionCompleted
        event in the history
        """
        d = {
            'decisionType': 'CompleteWorkflowExecution',
            'completeWorkflowExecutionDecisionAttributes': {
                'result': str_or_none(result)
            }
        }
        normalize_data(d)
        self._data.append(d)

    def fail_workflow_execution(self, reason=None, details=None):
        """
        Closes the workflow execution and records a WorkflowExecutionFailed
        event in the history.
        """
        d = {
            'decisionType': 'FailWorkflowExecution',
            'failWorkflowExecutionDecisionAttributes': {
                'reason': str_or_none(reason),
                'details': str_or_none(details)
            },
        }
        normalize_data(d)
        self._data.append(d)

    def cancel_workflow_execution(self, details=None):
        """
        Closes the workflow execution and records a WorkflowExecutionCanceled
        event in the history.
        """
        d = {
            'decisionType': 'CancelWorkflowExecution',
            'cancelWorkflowExecutionDecisionAttributes': {
                'details': str_or_none(details)
            }
        }
        normalize_data(d)
        self._data.append(d)

    def continue_as_new_workflow_execution(self,
                                           child_policy=None,
                                           execution_start_to_close_timeout=None,
                                           input=None,
                                           tag_list=None,
                                           task_list=None,
                                           task_priority=None,
                                           start_to_close_timeout=None,
                                           workflow_type_version=None,
                                           lambda_role=None):
        """
        Closes the workflow execution and starts a new workflow execution of
        the same type using the same workflow id and a unique run Id. A
        WorkflowExecutionContinuedAsNew event is recorded in the history.
        """
        d = {
            'decisionType': 'ContinueAsNewWorkflowExecution',
            'continueAsNewWorkflowExecutionDecisionAttributes': {
                'input': str_or_none(input),
                'executionStartToCloseTimeout': duration_encode(
                    execution_start_to_close_timeout, 'execution_start_to_close_timeout'),
                'taskList': {'name': str_or_none(task_list)},
                'taskPriority': str_or_none(task_priority),
                'taskStartToCloseTimeout': duration_encode(
                    start_to_close_timeout, 'start_to_close_timeout'),
                'childPolicy': cp_encode(child_policy),
                'tagList': tags_encode(tag_list),
                'workflowTypeVersion': str_or_none(workflow_type_version),
                'lambdaRole': str_or_none(lambda_role)
            }
        }
        normalize_data(d)
        self._data.append(d)

    def start_timer(self,
                    start_to_fire_timeout,
                    timer_id,
                    control=None):
        """
        Starts a timer for this workflow execution and records a TimerStarted
        event in the history.  This timer will fire after the specified delay
        and record a TimerFired event.

        :param int start_to_fire_timeout: the duration in seconds to wait before
            firing the timer
        :param timer_id: the unique ID of the timer
        :param control: data attached to the event that can be used by the
            decider in subsequent workflow tasks
        """
        d = {
            'decisionType': 'StartTimer',
            'startTimerDecisionAttributes': {
                'timerId': timer_id,
                'control': str_or_none(control),
                'startToFireTimeout': duration_encode(start_to_fire_timeout,
                                                      'start_to_fire_timeout')
            }
        }
        normalize_data(d)
        self._data.append(d)

    def cancel_timer(self, timer_id):
        """
        Cancels a previously started timer and records a TimerCanceled
        event in the history.

        :param timer_id: the unique ID of the timer
        """
        d = {
            'decisionType': 'CancelTimer',
            'cancelTimerDecisionAttributes': {
                'timerId': timer_id
            }
        }
        self._data.append(d)

    def signal_external_workflow_execution(self,
                                           workflow_id,
                                           signal_name,
                                           run_id=None,
                                           control=None,
                                           input=None):
        """
        Requests a signal to be delivered to the specified external workflow
        execution and records a SignalExternalWorkflowExecutionInitiated
        event in the history.

        :param str workflow_id: the id of the workflow execution to be signaled
        :param str signal_name: the name of the signal
        :param str run_id: The runId of the workflow execution to be signaled
        :param str control: data attached to event, used by the decider
        :param str input: input data to be provided with the signal
        """
        d = {
            'decisionType': 'SignalExternalWorkflowExecution',
            'signalExternalWorkflowExecutionDecisionAttributes': {
                'workflowId': workflow_id,
                'runId': str_or_none(run_id),
                'signalName': signal_name,
                'input': str_or_none(input),
                'control': str_or_none(control)

            }
        }
        normalize_data(d)
        self._data.append(d)

    def request_cancel_external_workflow_execution(self,
                                                   workflow_id,
                                                   control=None,
                                                   run_id=None):
        """
        Requests that a request be made to cancel the specified
        external workflow execution and records a
        RequestCancelExternalWorkflowExecutionInitiated event in the
        history.

        :param str workflow_id: the id of the workflow execution to cancel
        :param str run_id: The runId of the workflow execution to cancel
        :param str control: data attached to event, used by the decider
        """
        d = {
            'decisionType': 'RequestCancelExternalWorkflowExecution',
            'requestCancelExternalWorkflowExecutionDecisionAttributes': {
                'workflowId': workflow_id,
                'run_id': run_id,
                'control': control
            }
        }
        normalize_data(d)
        self._data.append(d)

    def start_child_workflow_execution(self,
                                       workflow_type_name,
                                       workflow_type_version,
                                       workflow_id,
                                       child_policy=None,
                                       control=None,
                                       execution_start_to_close_timeout=None,
                                       input=None,
                                       tag_list=None,
                                       task_list=None,
                                       task_priority=None,
                                       task_start_to_close_timeout=None,
                                       lambda_role=None):
        """
        Requests that a child workflow execution be started and
        records a StartChildWorkflowExecutionInitiated event in the
        history.  The child workflow execution is a separate workflow
        execution with its own history.
        """
        d = {
            'decisionType': 'StartChildWorkflowExecution',
            'startChildWorkflowExecutionDecisionAttributes': {
                'workflowType': {
                    'name': workflow_type_name,
                    'version': str(workflow_type_version)
                },
                'workflowId': workflow_id,
                'control': control,
                'input': input,
                'executionStartToCloseTimeout': duration_encode(
                    execution_start_to_close_timeout, 'execution_start_to_close_timeout'),
                'taskList': {'name': task_list},
                'taskPriority': task_priority,
                'taskStartToCloseTimeout': duration_encode(
                    task_start_to_close_timeout, 'task_start_to_close_timeout'),
                'childPolicy': cp_encode(child_policy),
                'tagList': tags_encode(tag_list),
                'lambdaRole': lambda_role,
            }
        }
        normalize_data(d)
        self._data.append(d)


def normalize_data(d):
    """Recursively goes through method kwargs and removes default values. This
    has side effect on the input.

    :type d: dict
    :param d: method's kwargs with default values to be removed
    """
    e = {}
    for key in list(d.keys()):
        if isinstance(d[key], dict):
            normalize_data(d[key])
        if d[key] in (None, e):
            del d[key]


def cp_encode(val):
    """Encodes and ensures value to a valid child policy.

    :param val: a valid child policy that has a `str` representation
    :rtype: str
    """
    if val is not None:
        val = str(val).upper()
        if val not in CHILD_POLICY.ALL:
            raise ValueError('Invalid child policy value: {}. Valid policies'
                             ' are {}.'.format(val, CHILD_POLICY.ALL))
    return val


def duration_encode(val, name, limit=None):
    """Encodes and validates duration used in several request parameters. For an
    indefinitely period use `DURATION.INDEF`.

    :param val: duration in strictly positive integer seconds or
        `DURATION.INDEF`/'NONE' for indefinitely
    :param name: parameter name to be encoded, used in error description
    :param limit: the upper limit to validate the duration in seconds
    :rtype: str
    """
    s_val = str(val).upper() if val else None
    if val is None or s_val == DURATION.INDEF:
        return val

    limit = limit or float('inf')
    err = ValueError('The value of {} must be a strictly positive integer and '
                     'lower than {} or "NONE": {} '.format(name, limit, val))
    try:
        val = int(val)
    except ValueError:
        raise err
    if not 0 < val < limit:
        raise err

    return s_val


def tags_encode(tags):
    """Encodes the list of tags. Trimmed to maximum 5 tags.

    :param tags: the list of tags; max 5 will be taken
    :rtype: list of str
    """
    if tags is None:
        return None
    return list(set(str(t) for t in tags))[:5]
