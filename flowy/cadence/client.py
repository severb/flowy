from flowy.cadence.cadence import cadence_frontend, shared

import tchannel

__all__ = ['SWFClient']

IDENTITY_SIZE = 256


class SWFClient(object):
    def __init__(self, tchannel):
        self.tchannel = tchannel

    def start_workflow_execution(self, domain, wid, name, version,
                                 task_list, request_id, input=None,
                                 execution_start_to_close_timeout=None,
                                 task_start_to_close_timeout=None,
                                 identity=None):
        if task_list is None:
            raise ValueError('Task List is required in Cadence')
        return self.tchannel.thrift(
            cadence_frontend.WorkflowService.StartWorkflowExecution(
                shared.StartWorkflowExecutionRequest(
                    domain,
                    wid,
                    shared.WorkflowType('%s-%s' % (name, version)),
                    shared.TaskList(task_list),
                    input,
                    execution_start_to_close_timeout,
                    task_start_to_close_timeout,
                    identity,
                    request_id
                )
            )
        ).result().body

    def poll_for_decision_task(self, domain, task_list, identity=None,
                               prev_request=None, max_page_size=1000):
        req = cadence_frontend.WorkflowService.PollForDecisionTask(
            shared.PollForDecisionTaskRequest(
                domain,
                shared.TaskList(task_list),
                identity
            )
        )
        if prev_request is not None:
            execution = prev_request.execution if hasattr(prev_request, 'execution') else prev_request.workflowExecution
            print 'execution: ', execution
            req = cadence_frontend.WorkflowService.GetWorkflowExecutionHistory(
                shared.GetWorkflowExecutionHistoryRequest(
                    domain=domain,
                    maximumPageSize=max_page_size,
                    nextPageToken=prev_request.nextPageToken,
                    execution=execution
                )
            )
        while 1:
            try:
                return self.tchannel.thrift(req, timeout=60*15).result().body
            except tchannel.errors.TimeoutError:
                pass

    def poll_for_activity_task(self, domain, task_list, identity=None):
        req = cadence_frontend.WorkflowService.PollForActivityTask(
            shared.PollForActivityTaskRequest(
                domain,
                shared.TaskList(task_list),
                identity
            )
        )
        while 1:
            try:
                return self.tchannel.thrift(req, timeout=60*15).result().body
            except tchannel.errors.TimeoutError:
                pass

    def record_activity_task_heartbeat(self, task_token, details=None):
        return self.tchannel.thrift(
            cadence_frontend.WorkflowService.RecordActivityTaskHeartbeat(
                shared.RecordActivityTaskHeartbeatRequest(
                    task_token,
                    details,
                )
            )
        ).result().body

    def respond_activity_task_failed(self, task_token, reason=None, details=None):
        return self.tchannel.thrift(
            cadence_frontend.WorkflowService.RespondActivityTaskFailed(
                shared.RespondActivityTaskFailedRequest(
                    task_token,
                    str(reason),
                    details,
                )
            )
        ).result().body

    def respond_activity_task_completed(self, task_token, result=None):
        return self.tchannel.thrift(
            cadence_frontend.WorkflowService.RespondActivityTaskCompleted(
                shared.RespondActivityTaskCompletedRequest(
                    task_token,
                    result,
                )
            )
        ).result().body

    def respond_decision_task_completed(self, task_token, decisions=None,
                                        exec_context=None):
        return self.tchannel.thrift(
            cadence_frontend.WorkflowService.RespondDecisionTaskCompleted(
                shared.RespondDecisionTaskCompletedRequest(
                    task_token,
                    decisions,
                    exec_context
                )
            )
        ).result().body


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
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.ScheduleActivityTask,
                scheduleActivityTaskDecisionAttributes=shared.ScheduleActivityTaskDecisionAttributes(
                    activity_id,
                    shared.ActivityType('%s-%s' % (activity_type_name, activity_type_version)),
                    None,  # XXX: why is domain here?
                    shared.TaskList(task_list),
                    input,
                    schedule_to_close_timeout,
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    heartbeat_timeout
                )
            )
        )

    def request_cancel_activity_task(self, activity_id):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.RequestCancelActivityTask,
                requestCancelActivityTaskDecisionAttributes=shared.RequestCancelActivityTaskDecisionAttributes(
                    activity_id
                )
            )
        )

    def record_marker(self, marker_name, details=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.RecordMarker,
                recordMarkerDecisionAttributes=shared.RecordMarkerDecisionAttributes(
                    marker_name,
                    details
                )
            )
        )

    def complete_workflow_execution(self, result=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.CompleteWorkflowExecution,
                completeWorkflowExecutionDecisionAttributes=shared.CompleteWorkflowExecutionDecisionAttributes(
                    result
                )
            )
        )

    def fail_workflow_execution(self, reason=None, details=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.FailWorkflowExecution,
                failWorkflowExecutionDecisionAttributes=shared.FailWorkflowExecutionDecisionAttributes(
                    str(reason),
                    details
                )
            )
        )

    def cancel_workflow_execution(self, details=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.CancelWorkflowExecution,
                cancelWorkflowExecutionDecisionAttributes=shared.CancelWorkflowExecutionDecisionAttributes(
                    details
                )
            )
        )

    def continue_as_new_workflow_execution(self, name, version,
                                           execution_start_to_close_timeout=None,
                                           input=None,
                                           task_list=None,
                                           start_to_close_timeout=None,
                                           workflow_type_version=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.ContinueAsNewWorkflowExecution,
                continueAsNewWorkflowExecutionDecisionAttributes=shared.ContinueAsNewWorkflowExecutionDecisionAttributes(
                    shared.WorkflowType('%s-%s' % (name, version)), # XXX: add workflow-type here
                    shared.TaskList(task_list),
                    input,
                    start_to_close_timeout,
                    execution_start_to_close_timeout,
                )
            )
        )

    def start_timer(self,
                    start_to_fire_timeout,
                    timer_id):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.StartTimer,
                startTimerDecisionAttributes=shared.StartTimerDecisionAttributes(
                    timer_id,
                    start_to_fire_timeout
                )
            )
        )

    def cancel_timer(self, timer_id):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.CancelTimer,
                cancelTimerDecisionAttributes=shared.CancelTimerDecisionAttributes(
                    timer_id,
                )
            )
        )

    def request_cancel_external_workflow_execution(self,
                                                   workflow_id,
                                                   control=None,
                                                   run_id=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.RequestCancelExternalWorkflowExecution,
                requestCancelActivityTaskDecisionAttributes=shared.RequestCancelActivityTaskDecisionAttributes(
                    None,
                    workflow_id,
                    run_id,
                    control,
                )
            )
        )


    def start_child_workflow_execution(self,
                                       workflow_type_name,
                                       workflow_type_version,
                                       workflow_id,
                                       child_policy=None,
                                       control=None,
                                       execution_start_to_close_timeout=None,
                                       input=None,
                                       task_list=None,
                                       task_start_to_close_timeout=None):
        self._data.append(
            shared.Decision(
                decisionType=shared.DecisionType.StartChildWorkflowExecution,
                startChildWorkflowExecutionDecisionAttributes=shared.StartChildWorkflowExecutionDecisionAttributes(
                    None,
                    workflow_id,
                    shared.WorkflowType('%s-%s' % (workflow_type_name, workflow_type_version)),
                    shared.TaskList(task_list),
                    input,
                    execution_start_to_close_timeout,
                    task_start_to_close_timeout,
                    child_policy,
                    control,
                )
            )
        )
