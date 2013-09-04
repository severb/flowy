class WorkflowContext(object):

    args_transport = JSONArgsTransport()
    result_transport = JSONResultTransport()

    def __init__(self, api_response):
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

    def encode_args_kwargs(self, args, kwargs):
        return self.args_transport.encode(args, kwargs)

    def get_execution_state(self):
        return WorkflowExecutionState(self.api_response)

    def execute(self, client, runner):
        runner_instance = runner(self.get_execution_state())
        scheduled_activities = runner_instance(*self.args, **self.kwargs)
        result = []
        for invocation_id, activity, args, kwargs in scheduled_activities:
            input = self.encode_args_kwargs(args, kwargs)
            result.append((invocation_id, activity, input))
        return result


class WorkflowExecutionState(object):

    result_transport = JSONResultTransport()

    def __init__(self, api_response):
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

    def result_value(self, result):
        return self.result_transport.value(result)

    def is_result_error(self, result):
        return self.result_transport.is_error(result)

    def result_for(self, invocation_id, default=None):
        event_result = self._event_result_by_invocation_id(invocation_id)
        if event_result is None:
            return default
        return self.result_value(event_result)

    def is_error(self, invocation_id):
        event_result = self._event_result_by_invocation_id(invocation_id)
        if event_result is None:
            return False
        return self.is_result_error(event_result)


class ActivityContext(object):

    args_transport = JSONArgsTransport()
    result_transport = JSONResultTransport()

    def __init__(self, api_response):
        self.api_response = api_response

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

    def encode_result(self, result):
        return self.result_transport.encode(result)

    def execute(self, runner):
        result = runner(*self.args, **self.kwargs)
        return self.result_transport(result)
