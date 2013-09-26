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
        MyWorkflow().resume('{"args": [], "kwargs": {}}', DummyContext() )

    def test_empty_workflow(self):

        class MyWorkflow(Workflow):
            def run(self):
                pass

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}', DummyContext()
        )
        self.assertEquals(set(s), set())
        self.assertEquals(r, 'null')

    def test_first_run(self):

        from pyswf.workflow import ActivityCall

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                self.f1()
                self.f1()
                self.f2()
                self.f2()

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}', DummyContext()
        )
        self.assertEquals(set(s), set([
            (0, 'f1', 'v1', '{"args": [], "kwargs": {}}'),
            (1, 'f1', 'v1', '{"args": [], "kwargs": {}}'),
            (2, 'f2', 'v2', '{"args": [], "kwargs": {}}'),
            (3, 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_no_reschedule(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                self.f1()
                self.f1()
                self.f2()
                self.f2()

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}', DummyContext(scheduled=[0, 1])
        )
        self.assertEquals(set(s), set([
            (2, 'f2', 'v2', '{"args": [], "kwargs": {}}'),
            (3, 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_activity_dependencies(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            f2 = ActivityProxy('f2', 'v2')
            def run(self):
                a = self.f1()
                b = self.f2(a)

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}', DummyContext()
        )
        self.assertEquals(set(s), set([
            (0, 'f1', 'v1', '{"args": [], "kwargs": {}}'),
        ]))


    def test_activity_args_serialization(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f', 'v')
            def run(self):
                a = self.f1(1, 2, a='b', c='d')

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}', DummyContext()
        )
        self.assertEquals(set(s), set([
            (0, 'f', 'v','{"args": [1, 2], "kwargs": {"a": "b", "c": "d"}}')
        ]))

    def test_workflow_input_deserialization(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self, a=None, b=None):
                self.f1(a, b)

        r, s = MyWorkflow().resume(
            '{"args": [1], "kwargs": {"b": 2}}', DummyContext()
        )
        self.assertEquals(set(s), set([
            (0, 'f1', 'v1','{"args": [1, 2], "kwargs": {}}')
        ]))

    def test_activity_result_deserialization(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                a, b = self.f1(), self.f1()
                self.f1(a.result() + b.result()[0] + b.result()[1])

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}',
            DummyContext(results={0: '1', 1: '[1, 2]'})
        )
        self.assertEquals(set(s), set([
            (2, 'f1', 'v1','{"args": [4], "kwargs": {}}')
        ]))

    def test_result_blocks_execution(self):

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                self.f1().result()
                self.f1()

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}', DummyContext()
        )
        self.assertEquals(set(s), set([
            (0, 'f1', 'v1', '{"args": [], "kwargs": {}}'),
        ]))

    def test_activity_error(self):
        from pyswf.workflow import _UnhandledActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                a = self.f1()

        my_workflow = MyWorkflow()
        self.assertRaises(
            _UnhandledActivityError,
            my_workflow.resume,
            '{"args": [], "kwargs": {}}',
            DummyContext(errors={0: 'error msg'})
        )

    def test_manual_activity_error(self):
        from pyswf.workflow import ActivityError

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

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}',
            DummyContext(errors={0: 'error msg'})
        )
        self.assertEquals(set(s), set([
            (1, 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_manual_activity_error_nesting(self):
        from pyswf.workflow import ActivityError

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

        r, s = MyWorkflow().resume(
            '{"args": [], "kwargs": {}}',
            DummyContext(errors={0: 'error msg'})
        )
        self.assertEquals(set(s), set([
            (1, 'f2', 'v2', '{"args": [], "kwargs": {}}'),
        ]))

    def test_manual_activity_error_args(self):
        from pyswf.workflow import _UnhandledActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                with self.error_handling():
                    a = self.f1()
                b = self.f1(a)

        my_workflow = MyWorkflow()
        self.assertRaises(
            _UnhandledActivityError,
            my_workflow.resume,
            '{"args": [], "kwargs": {}}',
            DummyContext(errors={0: 'error msg'})
        )

    def test_manual_activity_error_propagation(self):
        from pyswf.workflow import ActivityError

        class MyWorkflow(Workflow):
            f1 = ActivityProxy('f1', 'v1')
            def run(self):
                with self.error_handling():
                    a = self.f1()
                a.result()

        my_workflow = MyWorkflow()
        self.assertRaises(
            ActivityError,
            my_workflow.resume,
            '{"args": [], "kwargs": {}}',
            DummyContext(errors={0: 'error msg'})
        )

class DummyContext(object):
    def __init__(self, results={}, errors={}, scheduled=[], timedout=[]):
        self.results = results
        self.errors = errors
        self.scheduled = scheduled
        self.timedout = timedout

    def is_activity_scheduled(self, call_id):
        return call_id in self.scheduled

    def activity_result(self, call_id, default=None):
        return self.results.get(call_id, default)

    def activity_error(self, call_id, default=None):
        return self.errors.get(call_id, default)

    def is_activity_timedout(self, call_id):
        return call_id in self.timedout
