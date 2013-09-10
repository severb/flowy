class HistoryEvent(object):
    subdict = None

    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def _my_attributes(self):
        if self.subdict is None:
            return self.api_response
        return self.api_response[self.subdict]

    @property
    def event_type(self):
        return self.api_response['eventType']

    @property
    def event_id(self):
        return self.api_response['eventId']


class ActivityScheduled(HistoryEvent):
    subdict = 'activityTaskScheduledEventAttributes'

    @property
    def activity_id(self):
        return self._my_attributes['activityId']


class ActivityCompleted(HistoryEvent):
    subdict = 'activityTaskCompletedEventAttributes'

    @property
    def scheduled_event_id(self):
        return self._my_attributes['scheduledEventId']

    @property
    def result(self):
        return self._my_attributes['result']


class ActivityTimedOut(HistoryEvent):
    subdict = 'activityTaskTimedOutEventAttributes'

    @property
    def scheduled_event_id(self):
        return self._my_attributes['scheduledEventId']

    @property
    def type(self):
        return self._my_attributes['timeoutType']


class DecisionTask(object):
    def __init__(self, api_response):
        self.api_response = api_response
        self.next_decision_task = None

    def is_empty_response(self):
        return 'taskToken' not in self.api_response

    def chain_with(self, decision_task):
        self.next_decision_task = decision_task

    @property
    def next_page(self):
        return self.api_response.get('nextPageToken')

    @property
    def input(self):
        # This is a very naive approach for getting the workflow input.
        try:
            WESEA = 'workflowExecutionStartedEventAttributes'
            return self.api_response['events'][0][WESEA]['input']
        except KeyError:
            return self.next_decision_task.input

    @property
    def name(self):
        return self.api_response['workflowType']['name']

    @property
    def version(self):
        return self.api_response['workflowType']['version']

    @property
    def token(self):
        return self.api_response['taskToken']

    @property
    def events(self):
        m = {
            'ActivityTaskScheduled': ActivityScheduled,
            'ActivityTaskCompleted': ActivityCompleted,
            'ActivityTaskTimedOut': ActivityTimedOut
        }
        for event in self.api_response['events']:
            history_event = HistoryEvent(event)
            yield m.get(history_event.event_type, HistoryEvent)(event)
        if self.next_decision_task:
            for event in self.next_decision_task.events:
                yield event

    @property
    def scheduled_activities(self):
        for event in self.events:
            if event.event_type == 'ActivityTaskScheduled':
                yield event

    @property
    def completed_activities(self):
        for event in self.events:
            if event.event_type == 'ActivityTaskCompleted':
                yield event

    @property
    def timedout_activities(self):
        for event in self.events:
            if event.event_type == 'ActivityTaskTimedOut':
                yield event

    def scheduled_activity_by_event_id(self, id, default=None):
        for scheduled_activity in self.scheduled_activities:
            if scheduled_activity.event_id == id:
                return scheduled_activity
        return default

    def scheduled_activity_by_activity_id(self, id, default=None):
        for scheduled_activity in self.scheduled_activities:
            if scheduled_activity.activity_id == id:
                return scheduled_activity
        return default

    def completed_activity_by_scheduled_id(self, id, default=None):
        for completed_activity in self.completed_activities:
            if completed_activity.scheduled_event_id == id:
                return completed_activity
        return default

    def completed_activity_by_activity_id(self, id, default=None):
        sa = self.scheduled_activity_by_activity_id(id)
        if sa is not None:
            ca = self.completed_activity_by_scheduled_id(sa.event_id)
            if ca is not None:
                return ca
        return default

    def timedout_activity_by_scheduled_id(self, id, default=None):
        for timedout_activity in self.timedout_activities:
            if timedout_activity.scheduled_event_id == id:
                return timedout_activity
        return default

    def timedout_activity_by_activity_id(self, id, default=None):
        sa = self.scheduled_activity_by_activity_id(id)
        if sa is not None:
            ca = self.timedout_activity_by_scheduled_id(sa.event_id)
            if ca is not None:
                return ca
        return default


class ActivityTask(object):
    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def name(self):
        return self.api_response['activityType']['name']

    @property
    def version(self):
        return self.api_response['activityType']['version']

    @property
    def token(self):
        return self.api_response['taskToken']

    @property
    def input(self):
        return self.api_response['input']

    def is_empty_response(self):
        return 'taskToken' not in self.api_response


@implementer(IWorkflowEvent)
class WorkflowEvent(object):
    def __init__(self, api_response):
        self.api_response = api_response

    def update(self, context):
        raise NotImplementedError()


class ActivityScheduled(WorkflowEvent):
    def update(self, context):
        context.set_scheduled(self.api_response['scheduledEventId'])


class ActivityStarted(WorkflowEvent):
    def update(self, context):
        pass


class ActivityCompleted(WorkflowEvent):
    def update(self, context):
        context.set_result(
            self.api_response['scheduledEventId'], self.api_response['result']
        )


class SubworkflowStarted(WorkflowEvent):
    def update(self, context):
        pass
