from unittest import TestCase

from flowy.proxy import TaskProxy
from flowy.task import Workflow



class DummyWorkflow(Workflow):

    def __init__(self, *args, **kwargs):
        super(DummyWorkflow, self).__init__(*args, **kwargs)
        self.state = None

    def _flush(self):
        assert self.state is None, "Extra flush call."
        self.state = self._scheduled

    def _restart(self, input):
        assert self.state is None, "Extra restart call."
        self.state = [('RESTART', input)]

    def _complete(self, result):
        assert self.state is None, "Extra complete call."
        self.state = [('COMPLETE', result)]

    def _fail(self, reason):
        assert self.state is None, "Extra fail call."
        self.state = [('FAIL', str(reason))]

default_order = ['%s-%s' % (x, y) for x in range(100) for y in range(100)]


class TestWorkflow(TestCase):

    def run_workflow(self, running=[], timedout=[], results={}, errors={},
                     order=default_order, input='[[], {}]'):
        self.w = self.WF(input, running, timedout, results, errors, order)
        self.w()

    def assert_state(self, *state):
        self.assertEquals(self.w.state, list(state))


class TestSimpleWorkflow(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            return self.a()

    def test_initial_schedule(self):
        self.run_workflow()
        self.assert_state(
            (self.WF.a, '0-0', [], {}, 0)
        )

    def test_return_result(self):
        self.run_workflow(results={'0-0': '123'})
        self.assert_state(
            ('COMPLETE', '123')
        )

    def test_nothing(self):
        self.run_workflow(running=['0-0'])
        self.assert_state()

    def test_error(self):
        self.run_workflow(errors={'0-0': 'err!'})
        self.assert_state(
            ('FAIL', 'err!')
        )

    def test_default_timeout_retry(self):
        self.run_workflow(timedout=['0-0'])
        self.assert_state(
            (self.WF.a, '0-1', (), {}, 0)
        )

    def test_default_timeout_retry(self):
        self.run_workflow(timedout=['0-0', '0-1', '0-2'])
        self.assert_state(
            ('FAIL', 'A task has timedout.')
        )


class TestReturnEarly(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            self.a()
            self.a()

    def test_return(self):
        self.run_workflow()
        self.assert_state(
            ('COMPLETE', 'null')
        )


class TestDependency(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            return self.a(self.a(self.a()))

    def test_schedule_second(self):
        self.run_workflow(results={'0-0': '123'})
        self.assert_state(
            (self.WF.a, '1-0', [123], {}, 0)
        )

    def test_schedule_third(self):
        self.run_workflow(results={'0-0': '123', '1-0': '234'})
        self.assert_state(
            (self.WF.a, '2-0', [234], {}, 0)
        )

    def test_fast_lookup(self):
        # Make sure .result() is not called if not needed
        self.run_workflow(results={
            '0-0': 'invalid',
            '1-0': 'invalid',
            '2-0': '123'}
        )
        self.assert_state(
            ('COMPLETE', '123')
        )

    def test_fast_error_interrupt(self):
        # Make sure .result() is not called if not needed
        self.run_workflow(results={'0-0': '0'}, errors={'1-0': 'err!'})
        self.assert_state(
            ('FAIL', 'err!')
        )


class TestParallel(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            a1 = self.a(1)
            a2 = self.a(2)
            a3 = self.a(3)
            return self.a(a1.result(), a2.result(), a3.result())

    def test_initial_schedule(self):
        self.run_workflow()
        self.assert_state(
            (self.WF.a, '0-0', [1], {}, 0),
            (self.WF.a, '1-0', [2], {}, 0),
            (self.WF.a, '2-0', [3], {}, 0)
        )

    def test_dependency(self):
        self.run_workflow(results={'0-0': '10', '1-0': '20', '2-0': '30'})
        self.assert_state(
            (self.WF.a, '3-0', [10, 20, 30], {}, 0)
        )

    def test_dependency_complete(self):
        self.run_workflow(results={
            '0-0': '10',
            '1-0': '20',
            '2-0': '30',
            '3-0': '40'
        })
        self.assert_state(
            ('COMPLETE', '40')
        )


class TestRetry(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy(retry=[10, 20, 30])
        def run(self):
            return self.a()

    def test_custom_retry_10(self):
        self.run_workflow()
        self.assert_state(
            (self.WF.a, '0-0', [], {}, 10),
        )

    def test_custom_retry_20(self):
        self.run_workflow(timedout=['0-0'])
        self.assert_state(
            (self.WF.a, '0-1', [], {}, 20),
        )

    def test_custom_retry_30(self):
        self.run_workflow(timedout=['0-0', '0-1'])
        self.assert_state(
            (self.WF.a, '0-2', [], {}, 30),
        )

    def test_custom_retry_timeout(self):
        self.run_workflow(timedout=['0-0', '0-1', '0-2'])
        self.assert_state(
            ('FAIL', 'A task has timedout.')
        )


class TestFirst(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            return self.first(self.a(), self.a(), self.a())

    def test_first_1(self):
        self.run_workflow(results={'0-0': '10', '1-0': '20', '2-0': '30'},
                          order=['0-0', '1-0', '2-0'])
        self.assert_state(
            ('COMPLETE', '10')
        )

    def test_first_2(self):
        self.run_workflow(results={'0-0': '10', '1-0': '20', '2-0': '30'},
                          order=['1-0', '0-0', '2-0'])
        self.assert_state(
            ('COMPLETE', '20')
        )

    def test_first_3(self):
        self.run_workflow(results={'0-0': '10', '1-0': '20', '2-0': '30'},
                          order=['2-0', '1-0', '0-0'])
        self.assert_state(
            ('COMPLETE', '30')
        )

    def test_rest_running(self):
        self.run_workflow(results={'0-0': '10'}, running=['1-0', '2-0'],
                          order=['0-0'])
        self.assert_state(
            ('COMPLETE', '10')
        )


class TestFirstN(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            first_2 = self.first_n(2, self.a(), self.a(), self.a())
            return self.a(*first_2)

    def test_first_2(self):
        self.run_workflow(results={'0-0': '10', '1-0': '20', '2-0': '30'},
                          order=['2-0', '1-0', '0-0'])
        self.assert_state(
            (self.WF.a, '3-0', [30, 20], {}, 0),
        )

    def test_first_2_last_running(self):
        self.run_workflow(results={'1-0': '20', '2-0': '30'}, running=['0-0'],
                          order=['2-0', '1-0'])
        self.assert_state(
            (self.WF.a, '3-0', [30, 20], {}, 0),
        )

    def test_first_2_running(self):
        self.run_workflow(results={'2-0': '30'}, running=['0-0', '1-0'],
                          order=['2-0'])
        self.assert_state()


class TestErrorHandling(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy(error_handling=True)
        def run(self):
            return self.a(self.a(), self.a())

    def test_return_error(self):
        self.run_workflow(results={'0-0': '0', '1-0': '1'},
                          errors={'2-0': 'err!'})
        self.assert_state(
            ('FAIL', 'err!')
        )

    def test_fast_error_propagation(self):
        self.run_workflow(errors={'1-0': 'err!'})
        self.assert_state(
            ('FAIL', 'err!')
        )

    def test_error_on_result_load(self):
        self.run_workflow(results={'0-0': 'invalid'})
        self.assert_state(
            ('FAIL', 'No JSON object could be decoded')
        )

    def test_error_on_timeout(self):
        self.run_workflow(timedout=['0-0', '0-1', '0-2'])
        self.assert_state(
            ('FAIL', 'A task has timedout.')
        )


class TestOptions(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            with self.a.options(error_handling=True, retry=[1, 1, 1]):
                self.a()
            return self.a(self.a())

    def test_error_handling(self):
        self.run_workflow(errors={'0-0': 'err!'})
        self.assert_state(
            (self.WF.a, '1-0', [], {}, 0),
        )

    def test_retry(self):
        self.run_workflow()
        self.assert_state(
            (self.WF.a, '0-0', [], {}, 1),
            (self.WF.a, '1-0', [], {}, 0),
        )

    def test_revert_error_handling(self):
        self.run_workflow(errors={'1-0': 'err!'}, running=['0-0'])
        self.assert_state(
            ('FAIL', 'err!')
        )


class TestRestart(TestWorkflow):

    class WF(DummyWorkflow):
        a = TaskProxy()
        def run(self):
            from flowy.task import restart
            if self.a().result() > 10:
                return restart(1, 2)
            return 123

    def test_no_restart(self):
        self.run_workflow(results={"0-0": "1"})
        self.assert_state(
            ('COMPLETE', '123')
        )

    def test_restart(self):
        self.run_workflow(results={"0-0": "100"})
        self.assert_state(
            ('RESTART', '[[1, 2], {}]')
        )
