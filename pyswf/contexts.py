class WorkflowContext(object):
    def __init__(self, api_response):
        self.args_transport = JSONArgsTransport()
        self.result_transport = JSONResultTransport()
        self.api_response = api_response

    @property
    def id(self):
        return (
            self.api_response['workflowType']['name'],
            self.api_response['workflowType']['version']
        )

    @property
    def args(self):
        args, kwargs = self.args_transport.decode(
            "{'args': [], 'kwargs': {}}"
        )
        return args

    @property
    def kwargs(self):
        args, kwargs = self.args_transport.decode(
            "{'args': [], 'kwargs': {}}"
        )
        return kwargs

    def execute(self, client, runner):
        runner_instance = runner(WorkflowExecutionState(self.api_response))
        scheduled_activities = runner_instance(*self.args, **self.kwargs)
        # persist activities using client


class WorkflowExecutionState(object):
    def __init__(self, api_response):
        self.result_transport = JSONResultTransport()
        self.api_response = api_response

    @property
    def _events(self):
        return self.api_response['events']

    @property
    def _scheduled_activities(self):
        return filter(
            lambda e: e['eventType'] == 'ActivityTaskScheduled',
            self._events
        )

    @property
    def _completed_activities(self):
        return filter(
            lambda e: e['eventType'] == 'ActivityTaskCompleted',
            self._events
        )

    def is_scheduled(self, invocation_id):
        ATSEA = 'activityTaskScheduledEventAttributes'
        for event in self._scheduled_activities:
            if event[ATSEA]['activityId'] == invocation_id:
                return event
        return False

    def _event_result_by_invocation_id(self, invocation_id):
        schedule = self.is_scheduled(invocation_id)
        if not schedule:
            return None
        event_id = schedule['eventId']
        ATCEA = 'activityTaskCompletedEventAttributes'
        for event in self._completed_activities:
            if event[ATCEA]['scheduledEventId'] == event_id:
                    return event[ATCEA]['result']

    def result_for(self, invocation_id, default=None):
        event_result = self._event_result_by_invocation_id(invocation_id)
        if event_result is None:
            return default
        return self.result_transport.value(event_result)

    def is_error(self, invocation_id):
        event_result = self._event_result_by_invocation_id(invocation_id)
        if event_result is None:
            return False
        return self.result_transport.is_error(event_result)


class ActivityContext(object):
    def __init__(self, api_response):
        self.api_response = api_response
        self.args_transport = JSONArgsTransport()
        self.result_transport = JSONResultTransport()

    @property
    def id(self):
        return (
            self.api_response['activityType']['name'],
            self.api_response['activityType']['version']
        )

    @property
    def args(self):
        args, kwargs = self.args_transport.decode(self.api_response['input'])
        return args

    @property
    def kwargs(self):
        args, kwargs = self.args_transport.decode(self.api_response['input'])
        return kwargs

    def execute(self, runner):
        result = runner(*self.args, **self.kwargs)
        # persist result
