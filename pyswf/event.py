class WorkflowEvent(object):
    def __init__(self, api_response):
        self.api_response = api_response

    def update(self, context):
        pass


class ActivityScheduled(WorkflowEvent):
    def update(self, context):
        context.set_scheduled(self.api_response['scheduledEventId'])


class ActivityCompleted(WorkflowEvent):
    def update(self, context):
        context.set_result(
            self.api_response['scheduledEventId'], self.api_response['result']
        )


class ActivityFailed(WorkflowEvent):
    def update(self, context):
        context.set_error(
            self.api_response['scheduledEventId'], self.api_response['reason']
        )


class ActivityTimedOut(WorkflowEvent):
    def update(self, context):
        context.set_timeout(
            self.api_response['scheduledEventId']
        )


class WorkflowStarted(WorkflowEvent):
    pass
