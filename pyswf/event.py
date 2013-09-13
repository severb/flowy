class WorkflowEvent(object):
    def __init__(self, api_response):
        self.api_response = api_response

    def update(self, context):
        pass


class ActivityScheduled(WorkflowEvent):
    def update(self, context):
        event_id = self.api_response['eventId']
        subdict = self.api_response['activityTaskScheduledEventAttributes']
        call_id = subdict['activityId']
        context.set_scheduled(call_id, event_id)


class ActivityCompleted(WorkflowEvent):
    def update(self, context):
        subdict = self.api_response['activityTaskCompletedEventAttributes']
        context.set_result(subdict['scheduledEventId'], subdict['result'])


class ActivityFailed(WorkflowEvent):
    def update(self, context):
        subdict = self.api_response['activityTaskFailedEventAttributes']
        context.set_error(subdict['scheduledEventId'], subdict['reason'])


class ActivityTimedOut(WorkflowEvent):
    def update(self, context):
        subdict = self.api_response['activityTaskTimedOutEventAttributes']
        context.set_timed_out(subdict['scheduledEventId'])


class WorkflowStarted(WorkflowEvent):
    def update(self, context):
        subdict = self.api_response['workflowExecutionStartedEventAttributes']
        context.input = subdict['input']
