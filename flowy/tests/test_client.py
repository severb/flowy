import unittest


class DC(object):
    def __init__(self, err_type=0):
        self.err_type = err_type
        self.registered = []

    def register_workflow_type(self, domain, name, version, task_list,
                               child_policy='TERMINATE',
                               execution_start_to_close=30,
                               task_start_to_close=20, doc=None):
        if self.err_type == 1:
            from boto.swf.exceptions import SWFTypeAlreadyExistsError
            raise SWFTypeAlreadyExistsError(None, None)
        if self.err_type == 2:
            from boto.swf.exceptions import SWFResponseError
            raise SWFResponseError(None, None)
        self.registered.append((domain, name, version, task_list,
                                execution_start_to_close, task_start_to_close,
                                child_policy, doc))

    def describe_workflow_type(self, domain, name, version):
        return {'configuration': {
            'defaultExecutionStartToCloseTimeout': '12',
            'defaultTaskStartToCloseTimeout': '13',
            'defaultTaskList': {'name': 'taskl'},
            'defaultChildPolicy': 'TERMINATE'
        }}


class SWFClientTest(unittest.TestCase):

    def _get_uut(self, client, domain='domain', task_list='tasklist'):
        from flowy.client import SWFClient
        return SWFClient(domain, task_list, client)

    def test_registration(self):
        dummy_client = DC()
        c = self._get_uut(dummy_client, domain='dom', task_list='taskl')
        r = c.register_workflow(name='name', version=3,
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                doc='documentation')
        self.assertTrue(r)
        self.assertEquals(len(dummy_client.registered), 1)
        self.assertEquals(dummy_client.registered[0],
                          ('dom', 'name', '3', 'taskl', '12', '13',
                           'TERMINATE', 'documentation'))

    def test_already_registered(self):
        dummy_client = DC(err_type=1)
        c = self._get_uut(dummy_client, domain='dom', task_list='taskl')
        r = c.register_workflow(name='name', version=3,
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                doc='documentation')
        self.assertTrue(r)

    def test_registration_bad_defaults(self):
        c = self._get_uut(DC(err_type=1))
        self.assertFalse(
            c.register_workflow(name='name', version=3,
                                execution_start_to_close=1,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                doc='documentation')
        )
        self.assertFalse(
            c.register_workflow(name='name', version=3,
                                execution_start_to_close=12,
                                task_start_to_close=1,
                                child_policy='TERMINATE',
                                doc='documentation')
        )
        self.assertFalse(
            c.register_workflow(name='name', version=3,
                                execution_start_to_close=12,
                                task_start_to_close=1,
                                child_policy='BADPOLICY',
                                doc='documentation')
        )
        c = self._get_uut(DC(err_type=1), task_list='badlist')
        self.assertFalse(
            c.register_workflow(name='name', version=3,
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                doc='documentation')
        )

    def test_registration_unknown_error(self):
        c = self._get_uut(DC(err_type=2))
        r = c.register_workflow(name='name', version=3,
                                execution_start_to_close=12,
                                task_start_to_close=13,
                                child_policy='TERMINATE',
                                doc='documentation')
        self.assertFalse(r)


class WorkflowResponseTest(unittest.TestCase):

    def _get_uut(self, client):
        from flowy.client import WorkflowResponse
        return WorkflowResponse(client)

    def test_name_version(self):
        dummy_client = DummyClient(name='Name', version='123')
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual('Name', workflow_response.name)
        self.assertEqual('123', workflow_response.version)

    def test_scheduled_activities(self):
        dummy_client = DummyClient(token='token')
        workflow_response = self._get_uut(dummy_client)
        workflow_response.queue_activity('call1', 'activity1', 'v1', 'input1')
        workflow_response.queue_activity('call2', 'activity2', 'v2', 'input2')
        workflow_response.schedule_activities()
        self.assertEqual(set([
            ('call1', 'activity1', 'v1', 'input1'),
            ('call2', 'activity2', 'v2', 'input2'),
        ]), dummy_client.scheduled)
        self.assertEqual('token', dummy_client.token)

    def test_no_scheduled_activities(self):
        dummy_client = DummyClient(token='token')
        workflow_response = self._get_uut(dummy_client)
        workflow_response.schedule_activities()
        self.assertEqual(set(), dummy_client.scheduled)
        self.assertEqual('token', dummy_client.token)

    def test_complete_workflow(self):
        dummy_client = DummyClient(token='token')
        workflow_response = self._get_uut(dummy_client)
        workflow_response.complete_workflow('result')
        self.assertEqual('result', dummy_client.result)
        self.assertEqual('token', dummy_client.token)

    def test_teriminate_workflow(self):
        dummy_client = DummyClient(workflow_id='workflow_id')
        workflow_response = self._get_uut(dummy_client)
        workflow_response.terminate_workflow('reason')
        self.assertEqual('reason', dummy_client.reason)
        self.assertEqual('workflow_id', dummy_client.workflow_id)


class WorkflowNewEventsTest(unittest.TestCase):

    def _get_uut(self, client):
        from flowy.client import WorkflowResponse
        return WorkflowResponse(client)

    def test_first_few_events(self):
        dummy_client = DummyClient()
        for _ in range(4):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(len(list(workflow_response._new_events)), 5)

    def test_paginated_new_events(self):
        dummy_client = DummyClient(page_size=3)
        for _ in range(8):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(len(list(workflow_response._new_events)), 9)

    def test_first_few_new_events(self):
        dummy_client = DummyClient(previous_started_event_id='3')
        for _ in range(5):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(len(list(workflow_response._new_events)), 4)

    def test_first_few_paginated_new_events(self):
        dummy_client = DummyClient(page_size=3, previous_started_event_id='13')
        for _ in range(18):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(len(list(workflow_response._new_events)), 14)

    def test_no_new_events(self):
        dummy_client = DummyClient()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(len(list(workflow_response._new_events)), 1)


class WorkflowContextTest(unittest.TestCase):

    def _get_uut(self, client):
        from flowy.client import WorkflowResponse
        return WorkflowResponse(client)

    def test_no_context(self):
        dummy_client = DummyClient()
        for _ in range(4):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(workflow_response._context, None)

    def test_no_context_paginated(self):
        dummy_client = DummyClient()
        for _ in range(44):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(workflow_response._context, None)

    def test_context(self):
        dummy_client = DummyClient()
        workflow_response = self._get_uut(dummy_client)
        context = workflow_response._serialize_context()
        dummy_client = DummyClient(previous_started_event_id='5')
        dummy_client.add_dummy_event()
        dummy_client.add_decision_completed(context, '4', '3')
        for _ in range(5):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(workflow_response._context, context)

    def test_context_paginated(self):
        dummy_client = DummyClient()
        workflow_response = self._get_uut(dummy_client)
        context = workflow_response._serialize_context()
        dummy_client = DummyClient(previous_started_event_id='20', page_size=5)
        for _ in range(13):
            dummy_client.add_dummy_event()
        dummy_client.add_decision_completed(context, '4', '3')
        for _ in range(12):
            dummy_client.add_dummy_event()
        workflow_response = self._get_uut(dummy_client)
        self.assertEqual(workflow_response._context, context)


class DummyClient(object):
    def __init__(self,
        previous_started_event_id=None, page_size=10, token='token',
        input='', name='name', version='version', workflow_id='wfid'
    ):
        self.page_size = page_size
        self.events = [{
            'eventType': 'WorkflowExecutionStarted',
            'eventTimestamp': 1234567,
            'eventId': 'start',
            'workflowExecutionStartedEventAttributes': {
                'input': input,
            }
        }]
        self.current_token = None
        self.next_start = 0
        self.previous_started_event_id = previous_started_event_id
        self.token = token
        self.id = 0
        self.name = name
        self.version = version
        self.scheduled = set()
        self.workflow_id = workflow_id

    def add_event(event):
        self.events.append(event)

    def add_decision_completed(self, context, scheduled_id, started_id):
        d = {
            'eventType': 'DecisionTaskCompleted',
            'eventTimestamp': 1234567,
            'eventId': str(self.id),
            'decisionTaskCompletedEventAttributes': {
                'executionContext': context,
                'scheduledEventId': scheduled_id,
                'startedEventId': started_id
            }
        }
        self.id += 1
        self.events.append(d)

    def add_dummy_event(self):
        d = {
            'eventType': 'Dummy',
            'eventTimestamp': 1234567,
            'eventId': str(self.id),
        }
        self.id += 1
        self.events.append(d)

    def queue_activity(self, *args, **kwargs):
        self.scheduled.add(args)

    def schedule_activities(self, token, context):
        self.token = token
        self.context = context

    def terminate_workflow(self, workflow_id, reason):
        self.workflow_id = workflow_id
        self.reason = reason

    def complete_workflow(self, token, result):
        self.token = token
        self.result = result

    def poll_workflow(self, next_page_token=None):
        if not next_page_token == self.current_token:
            raise AssertionError('Invalid page token')
        r = self.events[self.next_start:self.next_start + self.page_size]
        self.next_start += self.page_size
        self.current_token = 'token_%s' % (self.next_start / self.page_size)
        page =  {
            'events': r,
            'workflowType': {'name': self.name, 'version': self.version},
            'taskToken': self.token,
            'workflowExecution': {'workflowId': self.workflow_id}
        }
        if len(self.events) > self.next_start:
            page['nextPageToken'] = self.current_token
        if self.previous_started_event_id is not None:
            page['previousStartedEventId'] = self.previous_started_event_id
        return page
