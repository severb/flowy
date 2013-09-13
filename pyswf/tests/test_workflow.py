import unittest

from pyswf.workflow import Workflow, ActivityProxy


class TestWorkflow(unittest.TestCase):

    def test_activity_proxy_identity(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                assert self.f1 is self.f1
                assert not (self.f1 is self.f2)
        response = DummyResponse()
        MyWorkflow(DummyContext(), response).resume()

    def test_empty_workflow(self):

        class MyWorkflow(Workflow):
            def run(self):
                pass

        response = DummyResponse()
        MyWorkflow(DummyContext(), response).resume()
        self.assertEquals(response.scheduled, set())

    def test_first_run(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                (self.f1(), self.f1(), self.f2(), self.f2())

        response = DummyResponse()
        MyWorkflow(DummyContext(), response).resume()
        self.assertEquals(response.scheduled, set([
            ('0', 'f1', 'v1', '{"args": [], "kwargs": {}}'),
            ('1', 'f1', 'v1', '{"args": [], "kwargs": {}}'),
            ('2', 'f2', 'v2', '{"args": [], "kwargs": {}}'),
            ('3', 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_no_reschedule(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                (self.f1(), self.f1(), self.f2(), self.f2())

        response = DummyResponse()
        MyWorkflow(DummyContext(scheduled=['0', '1']), response).resume()
        self.assertEquals(response.scheduled, set([
            ('2', 'f2', 'v2', '{"args": [], "kwargs": {}}'),
            ('3', 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_activity_dependencies(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                a = self.f1()
                b = self.f2(a)

        response = DummyResponse()
        MyWorkflow(DummyContext(), response).resume()
        self.assertEquals(response.scheduled, set([
            ('0', 'f1', 'v1', '{"args": [], "kwargs": {}}'),
        ]))

    def test_activity_args_serialization(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                a = self.f1(1, 2, a='b', c='d')

        response = DummyResponse()
        MyWorkflow(DummyContext(), response).resume()
        self.assertEquals(response.scheduled, set([(
            '0', 'f1', 'v1','{"args": [1, 2], "kwargs": {"a": "b", "c": "d"}}'
        )]))

    def test_workflow_input_deserialization(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self, a=None, b=None):
                self.f1(a, b)

        response = DummyResponse()
        MyWorkflow(
            DummyContext(input='{"args": [1], "kwargs": {"b": 2}}'),
            response
        ).resume()
        self.assertEquals(response.scheduled, set([
            ('0', 'f1', 'v1','{"args": [1, 2], "kwargs": {}}')
        ]))

    def test_activity_result_deserialization(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self, a=None, b=None):
                a, b = self.f1(), self.f1()
                self.f1(a.result() + b.result()[0] + b.result()[1])

        response = DummyResponse()
        MyWorkflow(
            DummyContext(results={'0': '1', '1': '[1, 2]'}),
            response
        ).resume()
        self.assertEquals(response.scheduled, set([
            ('2', 'f1', 'v1','{"args": [4], "kwargs": {}}')
        ]))

    def test_result_blocks_execution(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                self.f1().result()
                self.f1()

        response = DummyResponse()
        MyWorkflow(DummyContext(), response).resume()
        self.assertEquals(response.scheduled, set([
            ('0', 'f1', 'v1', '{"args": [], "kwargs": {}}'),
        ]))

    def test_activity_error(self):
        from pyswf.activity import ActivityError
        from pyswf.workflow import _UnhandledActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                a = self.f1()

        my_workflow = MyWorkflow(
            DummyContext(errors={'0': 'error msg'}),
            DummyResponse()
        )
        self.assertRaises(_UnhandledActivityError, my_workflow.resume)

    def test_manual_activity_error(self):
        from pyswf.activity import ActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                with self.error_handling():
                    a = self.f1()
                try:
                    a.result()
                except ActivityError:
                    self.f2()

        response = DummyResponse()
        context = DummyContext(errors={'0': 'error msg'})
        MyWorkflow(context, response).resume()
        self.assertEquals(response.scheduled, set([
            ('1', 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_manual_activity_error_nesting(self):
        from pyswf.activity import ActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                with self.error_handling():
                    with self.error_handling():
                        with self.error_handling():
                            pass
                    a = self.f1()
                try:
                    a.result()
                except ActivityError:
                    self.f2()

        response = DummyResponse()
        context = DummyContext(errors={'0': 'error msg'})
        MyWorkflow(context, response).resume()
        self.assertEquals(response.scheduled, set([
            ('1', 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_manual_activity_error_args(self):
        from pyswf.activity import ActivityError
        from pyswf.workflow import _UnhandledActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                with self.error_handling():
                    a = self.f1()
                b = self.f1(a)

        my_workflow = MyWorkflow(
            DummyContext(errors={'0': 'error msg'}),
            DummyResponse()
        )
        self.assertRaises(_UnhandledActivityError, my_workflow.resume)


class DummyResponse(object):
    def __init__(self):
        self.scheduled = set()

    def schedule(self, call_id, a_name, a_version, input):
        self.scheduled.add((call_id, a_name, a_version, input))


class DummyContext(object):
    def __init__(self,
            results={}, errors={}, scheduled=[], timedout=[],
            input='{"args": [], "kwargs": {}}'
    ):
        self.results = results
        self.errors = errors
        self.scheduled = scheduled
        self.timedout = timedout
        self.input = input

    def is_activity_scheduled(self, call_id):
        return call_id in self.scheduled

    def activity_result(self, call_id, default=None):
        return self.results.get(call_id, default)

    def activity_error(self, call_id, default=None):
        return self.errors.get(call_id, default)

    def is_activity_timedout(self, call_id):
        return call_id in self.timedout
