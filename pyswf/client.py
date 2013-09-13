from boto.swf.layer1 import Layer1
from boto.swf.exceptions import SWFTypeAlreadyExistsError

from pyswf.event import (
    WorkflowEvent, ActivityScheduled, ActivityCompleted, ActivityFailed,
    ActivityTimedOut, WorkflowStarted
)


class WorkflowClient(object):

    events = {
        'ActivityTaskScheduled': ActivityScheduled,
        'ActivityTaskCompleted': ActivityCompleted,
        'ActivityTaskFailed': ActivityFailed,
        'ActivityTaskTimedOut': ActivityTimedOut,
        'WorkflowExecutionStarted': WorkflowStarted
    }

    def __init__(self, domain, task_list, client=None):
        self.client = client if client is not None else Layer1()
        self.domain = domain
        self.task_list = task_list
        self.workflows = {}

    def register(self, name, version, workflow_runner,
        execution_start_to_close=3600,
        task_start_to_close=60,
        child_policy='TERMINATE',
        doc=None
    ):
        self.workflows[(name, version)] = workflow_runner
        try:
            self.client.register_workflow_type(
                self.domain,
                name,
                version,
                self.task_list,
                child_policy,
                execution_start_to_close,
                task_start_to_close,
                doc
            )
        except SWFTypeAlreadyExistsError:
            pass # Check if the registered workflow has the same properties.

    def schedule_activities(self, token, activities, context=None):
        d = Layer1Decisions()
        for call_id, activity_name, activity_version, input in activities:
            d.schedule_activity_task(
                call_id, activity_name, activity_version, input
            )
        self.client.respond_decision_task_completed(
            token, decisions=d._data, execution_context=context
        )

    def complete_workflow(self, token, result):
        d = Layer1Decisions()
        d.complete_workflow_execution(result=result)
        self.client.respond_decision_task_completed(token, decisions=d._data)

    def terminate_workflow(self, workflow_id, reason):
        self.client.terminate_workflow_execution(
            self.domain, workflow_id, reason=reason
        )

    def query_event(self, event_data, default=WorkflowEvent):
        event_type = event_data['eventType']
        return self.events.get(event_type, default)

    def run(self):
        while 1:
            response = WorkflowResponse(self)
            context = WorkflowContext(response.context)
            for event_data in response.new_events:
                event = self.query_event(event_data)
                event.update(context)
            runner = self._query(response.name, response.version)
            try:
                result = runner.resume(response, context)
            except ActivityError as e:
                response.terminate(e.message)
            except _UnhandledActivityError as e:
                response.terminate(e.message)
            else:
                running = context.any_activity_running()
                scheduled = response.any_activity_scheduled()
                if not (running or scheduled):
                    response.complete(result)
                else:
                    response.suspend(context.serialize())

    def poll(self, next_page_token=None):
        return self.client.poll_for_decision_task(
            self.domain, self.task_list,
            reverse_order=True, next_page_token=next_page_token
        )

    def _query(self, name, version):
        return self.workflows[(name, version)]


class WorkflowResponse(object):
    def __init__(self, client):
        self.client = client
        self.scheduled = []
        self._cached_api_response = None

    @property
    def _api_response(self):
        if self._cached_api_response is None:
            self._cached_api_response = self.client.poll()
        return self._cached_api_response

    @property
    def name(self):
        return self._api_response['workflowType']['name']

    @property
    def version(self):
        return self._api_response['workflowType']['version']

    @property
    def context(self):
        for event in self.new_events:
            if event['eventType'] == 'DecisionTaskCompleted':
                DTCEA = 'decisionTaskCompletedEventAttributes'
                return event[DTCEA]['executionContext']
        return None

    def any_activity_scheduled(self):
        return bool(self.scheduled)

    @property
    def _t(self):
        return self._api_response['taskToken']

    def schedule(self, call_id, activity_name, activity_version, input):
        self.scheduled.append(
            (call_id, activity_name, activity_version, input)
        )

    def suspend(self, context):
        self.client.schedule_activities(self._t, self.scheduled, context)

    def complete(self, result):
        self.client.complete_workflow(self._t, result)

    def terminate(self, reason):
        workflow_id = self.api_response['workflowExecution']['workflowId']
        self.client.terminate_workflow_execution(workflow_id ,reason)

    @property
    def new_events(self):
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

    @property
    def _events(self):
        api_response = self._api_response
        for event in api_response['events']:
            yield event
        while api_response.get('nextPageToken'):
            api_response = self.client.poll(
                next_page_token=api_response['nextPageToken'],
            )
            for event in api_response['events']:
                yield event
