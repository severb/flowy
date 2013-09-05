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


class DecisionTask(object):
    def __init__(self, api_response):
        self.api_response = api_response

    def is_empty_response(self):
        return 'taskToken' not in self.api_response

    @property
    def input(self):
        # This is a very naive approach for getting the workflow input.
        WESEA = 'workflowExecutionStartedEventAttributes'
        return self.api_response['events'][0][WESEA]['input']

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
            'ActivityTaskCompleted': ActivityCompleted
        }
        for event in self.api_response['events']:
            history_event = HistoryEvent(event)
            yield m.get(history_event.event_type, HistoryEvent)(event)

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
