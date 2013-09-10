import unittest


class WorkflowResponseTest(unittest.TestCase):

    def _get_uut(self, api_response, domain=None, task_list=None, client=None):
        from pyswf.client import WorkflowResponse
        return WorkflowResponse(api_response, client)

    def test_name_version(self):
        api_response = {'workflowType': {'name': 'Name', 'version': '123'}}
        workflow_response = self._get_uut(api_response)
        self.assertEqual('Name', workflow_response.name)
        self.assertEqual('123', workflow_response.version)

    def test_scheduled_activities(self):
        dummy_client = DummyClient()
        api_response = {'taskToken': 'token'}
        workflow_response = self._get_uut(api_response, client=dummy_client)
        workflow_response.schedule('call1', 'activity1', 'v1', 'input1')
        workflow_response.schedule('call2', 'activity2', 'v2', 'input2')
        workflow_response.suspend('context')
        self.assertEqual(set([
            ('call1', 'activity1', 'v1', 'input1'),
            ('call2', 'activity2', 'v2', 'input2'),
        ]), dummy_client.scheduled)
        self.assertEqual('context', dummy_client.context)
        self.assertEqual('token', dummy_client.token)

    def test_no_scheduled_activities(self):
        dummy_client = DummyClient()
        api_response = {'taskToken': 'token'}
        workflow_response = self._get_uut(api_response, client=dummy_client)
        workflow_response.suspend('context')
        self.assertEqual(set(), dummy_client.scheduled)
        self.assertEqual('context', dummy_client.context)
        self.assertEqual('token', dummy_client.token)

    def test_complete_workflow(self):
        dummy_client = DummyClient()
        api_response = {'taskToken': 'token'}
        workflow_response = self._get_uut(api_response, client=dummy_client)
        workflow_response.complete('result')
        self.assertEqual('result', dummy_client.result)
        self.assertEqual('token', dummy_client.token)


class WorkflowContextTest(unittest.TestCase):

    def _get_uut(self, api_response, domain=None, task_list=None, client=None):
        from pyswf.client import WorkflowResponse
        return WorkflowResponse(api_response, client)

    def test_no_context(self):
        dtc = {'eventType': 'DecisionTaskCompleted'}


class DummyClient(object):
    def __init__(self):
        pass

    def schedule_activities(self, token, scheduled, context):
        self.token = token
        self.scheduled = set(scheduled)
        self.context = context

    def complete_workflow(self, token, result):
        self.token = token
        self.result = result
