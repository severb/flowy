import json
import uuid

from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions
from boto.swf.exceptions import (
    SWFTypeAlreadyExistsError, SWFTypeAlreadyExistsError
)

from pyswf.workflow import _UnhandledActivityError, ActivityError


__all__ = ['ActivityClient', 'WorkflowClient']


class SWFClient(object):
    def __init__(self, domain, task_list, client=None):
        self.client = client if client is not None else Layer1()
        self.domain = domain
        self.task_list = task_list
        self.scheduled_activities = []

    def register_workflow(self, name, version, workflow_runner,
        execution_start_to_close=3600,
        task_start_to_close=60,
        child_policy='TERMINATE',
        doc=None
    ):
        try:
            self.client.register_workflow_type(
                self.domain,
                name,
                str(version),
                self.task_list,
                child_policy,
                str(execution_start_to_close),
                str(task_start_to_close),
                doc
            )
        except SWFTypeAlreadyExistsError:
            pass # Check if the registered workflow has the same properties.

    def queue_activity(
        self, call_id, name, version, input,
        heartbeat=None,
        schedule_to_close=None,
        schedule_to_start=None,
        start_to_close=None,
        task_list=None
    ):
        self.scheduled_activities.append((
            (str(call_id), name, str(version)),
            {
                'heartbeat_timeout': _str_or_none(heartbeat),
                'schedule_to_close_timeout': _str_or_none(schedule_to_close),
                'schedule_to_start_timeout': _str_or_none(schedule_to_start),
                'start_to_close_timeout': _str_or_none(start_to_close),
                'task_list': task_list,
                'input': input
            }
        ))

    def schedule_activities(self, token, context=None):
        d = Layer1Decisions()
        for args, kwargs in self.scheduled_activities:
            d.schedule_activity_task(*args, **kwargs)
        self.client.respond_decision_task_completed(
            token, decisions=d._data, execution_context=context
        )
        self.scheduled_activities = []

    def complete_workflow(self, token, result):
        d = Layer1Decisions()
        d.complete_workflow_execution(result=result)
        self.client.respond_decision_task_completed(token, decisions=d._data)

    def terminate_workflow(self, workflow_id, reason):
        self.client.terminate_workflow_execution(
            self.domain, workflow_id, reason=reason
        )

    def poll_workflow(self, next_page_token=None):
        return self.client.poll_for_decision_task(
            self.domain, self.task_list,
            reverse_order=True, next_page_token=next_page_token
        )

    def request_workflow(self):
        return WorkflowResponse(self)

    def register_activity(self, name, version, activity_runner,
        heartbeat=30,
        schedule_to_close=300,
        schedule_to_start=60,
        start_to_close=120,
        doc=None
    ):
        try:
            self.client.register_activity_type(
                self.domain,
                name,
                str(version),
                self.task_list,
                str(heartbeat),
                str(schedule_to_close),
                str(schedule_to_start),
                str(start_to_close),
                doc
            )
        except SWFTypeAlreadyExistsError:
            pass # Check if the registered activity has the same properties.

    def poll_activity(self):
        return self.client.poll_for_activity_task(self.domain, self.task_list)

    def complete_activity(self, token, result):
        self.client.respond_activity_task_completed(token, result)

    def terminate_activity(self, token, reason):
        self.client.respond_activity_task_failed(token, reason=reason)

    def request_activity(self):
        return ActivityResponse(self)

    def start_workflow(self, name, version, input):
        self.client.start_workflow_execution(
            self.domain, str(uuid.uuid4()), name, str(version),
            task_list=self.task_list, input=input
        )


class WorkflowResponse(object):
    def __init__(self, client):
        self.client = client
        self._event_to_call_id = {}
        self._scheduled = set()
        self._results = {}
        self._timed_out = set()
        self._with_errors = {}
        self.input = None

        response = {}
        while 'taskToken' not in response or not response['taskToken']:
            response = self.client.poll_workflow()
        self._api_response = response

        if self._context is not None:
            initial_state = json.loads(self._context)
            self._event_to_call_id = self.fix_keys(
                initial_state['event_to_call_id']
            )
            self._scheduled = set(initial_state['scheduled'])
            self._results = self.fix_keys(initial_state['results'])
            self._timed_out = set(initial_state['timed_out'])
            self._with_errors = self.fix_keys(initial_state['with_errors'])
            self.input = initial_state['input']

        # Update the context with all new events
        for new_event in self._new_events:
            event_type = new_event['eventType']
            getattr(self, '_%s' % event_type, lambda x: 0)(new_event)

        # Assuming workflow started is always the first event
        assert self.input is not None

    @staticmethod
    def fix_keys(d):
        # Fix json's stupid silent key conversion from int to string
        return dict((int(key), value) for key, value in d.items())

    @property
    def name(self):
        return self._api_response['workflowType']['name']

    @property
    def version(self):
        return self._api_response['workflowType']['version']

    def queue_activity(
        self, call_id, name, version, input,
        heartbeat=None,
        schedule_to_close=None,
        schedule_to_start=None,
        start_to_close=None,
        task_list=None
    ):
        self.client.queue_activity(
            call_id, name, version, input,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_list=task_list
        )

    def schedule_activities(self):
        self.client.schedule_activities(self._token, self._serialize_context())

    def complete_workflow(self, result):
        self.client.complete_workflow(self._token, result)

    def terminate_workflow(self, reason):
        workflow_id = self._api_response['workflowExecution']['workflowId']
        self.client.terminate_workflow(workflow_id ,reason)

    def any_activity_running(self):
        return bool(self._scheduled)

    def is_activity_scheduled(self, call_id):
        return call_id in self._scheduled

    def activity_result(self, call_id, default=None):
        return self._results.get(call_id, default)

    def activity_error(self, call_id, default=None):
        return self._with_errors.get(call_id, default)

    def is_activity_timeout(self, call_id):
        return call_id in self._timed_out

    @property
    def _token(self):
        return self._api_response['taskToken']

    @property
    def _context(self):
        for event in self._new_events:
            if event['eventType'] == 'DecisionTaskCompleted':
                DTCEA = 'decisionTaskCompletedEventAttributes'
                return event[DTCEA]['executionContext']
        return None

    def _serialize_context(self):
        return json.dumps({
            'event_to_call_id': self._event_to_call_id,
            'scheduled': list(self._scheduled),
            'results': self._results,
            'timed_out': list(self._timed_out),
            'with_errors': self._with_errors,
            'input': self.input,
        })

    @property
    def _events(self):
        if not hasattr(self, '_cached_events'):
            events = []
            api_response = self._api_response
            while api_response.get('nextPageToken'):
                for event in api_response['events']:
                    events.append(event)
                api_response = self.client.poll_workflow(
                    next_page_token=api_response['nextPageToken']
                )
            for event in api_response['events']:
                events.append(event)
            self._cached_events = events
        return self._cached_events

    @property
    def _new_events(self):
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

    def _ActivityTaskScheduled(self, event):
        event_id = event['eventId']
        subdict = event['activityTaskScheduledEventAttributes']
        call_id = int(subdict['activityId'])
        self._event_to_call_id[event_id] = call_id
        self._scheduled.add(call_id)

    def _ActivityTaskCompleted(self, event):
        subdict = event['activityTaskCompletedEventAttributes']
        event_id, result = subdict['scheduledEventId'], subdict['result']
        self._scheduled.remove(self._event_to_call_id[event_id])
        self._results[self._event_to_call_id[event_id]] = result

    def _ActivityTaskFailed(self, event):
        subdict = event['activityTaskFailedEventAttributes']
        event_id, reason = subdict['scheduledEventId'], subdict['reason']
        self._with_errors[self._event_to_call_id[event_id]] = reason

    def _ActivityTaskTimedOut(self, event):
        subdict = event['activityTaskTimedOutEventAttributes']
        event_id = subdict['scheduledEventId']
        self._scheduled.remove(self._event_to_call_id[event_id])
        self._timed_out.add(self._event_to_call_id[event_id])

    def _WorkflowExecutionStarted(self, event):
        subdict = event['workflowExecutionStartedEventAttributes']
        self.input = subdict['input']


class WorkflowLoop(object):
    def __init__(self, client):
        self.client = client
        self.workflows = {}

    def register(self, name, version, workflow_runner,
        execution_start_to_close=3600,
        task_start_to_close=60,
        child_policy='TERMINATE',
        doc=None
    ):
        self.workflows[(name, str(version))] = workflow_runner
        self.client.register_workflow(name, version, workflow_runner,
            execution_start_to_close, task_start_to_close, child_policy, doc
        )

    def start(self):
        while 1:
            response = self.client.request_workflow()
            workflow_runner = self._query(response.name, response.version)
            try:
                result, activities = workflow_runner.resume(
                    response.input, response
                )
            except ActivityError as e:
                response.terminate_workflow(e.message)
            except _UnhandledActivityError as e:
                response.terminate_workflow(e.message)
            else:
                activities_running = response.any_activity_running()
                activities_scheduled = bool(activities)
                if activities_running or activities_scheduled:
                    for a in activities:
                        response.queue_activity(
                            a.call_id, a.name, a.version, a.input,
                            heartbeat=a.options.heartbeat,
                            schedule_to_close=a.options.schedule_to_close,
                            schedule_to_start=a.options.schedule_to_start,
                            start_to_close=a.options.start_to_close,
                            task_list=a.options.task_list
                        )
                    response.schedule_activities()
                else:
                    response.complete_workflow(result)

    def _query(self, name, version):
        return self.workflows[(name, version)]


class WorkflowClient(object):
    def __init__(self, loop):
        self.loop = loop

    @classmethod
    def for_domain(cls, domain, task_list):
        client = SWFClient(domain, task_list)
        loop = WorkflowLoop(client)
        return cls(loop)

    @classmethod
    def from_client(cls, client):
        loop = WorkflowLoop(client)
        return cls(loop)

    def __call__(self, name, version, *args, **kwargs):
        optional_args = [
            'execution_start_to_close',
            'task_start_to_close',
            'child_policy',
        ]
        r_kwargs = {}
        for arg_name in optional_args:
            arg_value = kwargs.pop(arg_name, None)
            if arg_value is not None:
                r_kwargs[arg_name] = arg_value
        def wrapper(workflow):
            r_kwargs['doc'] = workflow.__doc__.strip()
            self.loop.register(
                name, version, workflow(*args, **kwargs), **r_kwargs
            )
            return workflow
        return wrapper

    def start(self):
        self.loop.start()


class ActivityResponse(object):
    def __init__(self, client):
        self.client = client
        response = self.client.poll_activity()
        while 'taskToken' not in response or not response['taskToken']:
            response = self.client.poll_activity()
        self._api_response = response

    def complete(self, result):
        self.client.complete_activity(self._token, result)

    def terminate(self, reason):
        self.client.terminate_activity(self._token, reason)

    @property
    def name(self):
        return self._api_response['activityType']['name']

    @property
    def version(self):
        return self._api_response['activityType']['version']

    @property
    def input(self):
        return self._api_response['input']

    @property
    def _token(self):
        return self._api_response['taskToken']


class ActivityLoop(object):
    def __init__(self, client):
        self.client = client
        self.activities = {}

    def register(self, name, version, activity_runner,
        heartbeat=30,
        schedule_to_close=300,
        schedule_to_start=60,
        start_to_close=120,
        doc=None
    ):
        # All versions are converted to string in SWF and that's how we should
        # store them too in order to be able to query for them
        self.activities[(name, str(version))] = activity_runner
        self.client.register_activity(
            name, version, activity_runner, heartbeat,
            schedule_to_close, schedule_to_start, start_to_close, doc
        )

    def start(self):
        while 1:
            response = self.client.request_activity()
            activity_runner = self._query(response.name, response.version)
            try:
                result = activity_runner.call(response.input)
            except Exception as e:
                response.terminate(e.message)
            else:
                response.complete(result)

    def _query(self, name, version):
        # XXX: if we can't resolve this activity log the error and continue
        return self.activities[(name, version)]


class ActivityClient(object):
    def __init__(self, loop):
        self.loop = loop

    @classmethod
    def for_domain(cls, domain, task_list):
        client = SWFClient(domain, task_list)
        loop = ActivityLoop(client)
        return cls(loop)

    @classmethod
    def from_client(cls, client):
        loop = ActivityLoop(client)
        return cls(loop)

    def __call__(self, name, version, *args, **kwargs):
        version = str(version)
        optional_args = [
            'heartbeat',
            'schedule_to_close',
            'schedule_to_start',
            'start_to_close',
        ]
        r_kwargs = {}
        for arg_name in optional_args:
            arg_value = kwargs.pop(arg_name, None)
            if arg_value is not None:
                r_kwargs[arg_name] = str(arg_value)
        def wrapper(activity):
            r_kwargs['doc'] = activity.__doc__.strip()
            self.loop.register(
                name, version, activity(*args, **kwargs), **r_kwargs
            )
            return activity
        return wrapper

    def start(self):
        self.loop.start()


class WorkflowStarter(object):
    def __init__(self, client):
        self.client = client

    @classmethod
    def for_domain(cls, domain, task_list):
        client = SWFClient(domain, task_list)
        return cls(client)

    def start(self, name, version, *args, **kwargs):
        input = json.dumps({"args": args, "kwargs": kwargs})
        self.client.start_workflow(name, version, input)


def _str_or_none(maybe_none):
    return maybe_none is not None and str(maybe_none)
