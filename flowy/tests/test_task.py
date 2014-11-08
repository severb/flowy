from unittest import TestCase


class DummyScheduler(object):

    def __init__(self):
        self.state = []

    def flush(self):
        self.state.append('FLUSH')

    def restart(self, spec, input, tags):
        self.state.append('RESTART', spec, input, tags)

    def fail(self, reason):
        self.state.append(('FAIL', str(reason)))

    def complete(self, result):
        self.state.append(('COMPLETE', result))

    def schedule_timer(self, delay, call_id):
        self.state.append(('TIMER', delay, call_id))

    def schedule_activity(self, spec, call_id, input):
        self.state.append(('ACTIVITY', spec, call_id, input))

    def schedule_workflow(self, spec, call_id, input):
        self.state.append(('WORKFLOW', spec, call_id, input))


class TestWorkflowScheduling(TestCase):

    def set_state(self, running=[], timedout=[], results={}, errors={},
                  order=None):
        from flowy.task import _SWFWorkflow
        self.scheduler = DummyScheduler()
        if order is None:
            order = list(range(100000))
        self.workflow = _SWFWorkflow(self.scheduler, 'input', 'token', running,
                                     timedout, results, errors, order, None,
                                     None)
        self.RUNNING = self.workflow._RUNNING
        self.FOUND = self.workflow._FOUND
        self.ERROR = self.workflow._ERROR
        self.TIMEDOUT = self.workflow._TIMEDOUT
        self.state = []

    def schedule_activity(self, spec, input='i', retry=0, delay=0):
        r = self.workflow.schedule_activity(spec, input, retry, delay)
        self.state.append(r)

    def schedule_workflow(self, spec, input='i', retry=0, delay=0):
        r = self.workflow.schedule_workflow(spec, input, retry, delay)
        self.state.append(r)

    def assert_state(self, *state):
        self.assertEquals(list(state), self.state)

    def assert_scheduled(self, *l):
        self.assertEquals(list(l), self.scheduler.state)

    def test_simple_scheduling_not_found(self):
        self.set_state(running=[], timedout=[], results={}, errors={})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
            (self.RUNNING, None, None)
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 0, 'in1'),
            ('WORKFLOW', 'w1', 1, 'in2'),
        )

    def test_simple_scheduling_running(self):
        self.set_state(running=[0, 1])
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
            (self.RUNNING, None, None)
        )
        self.assert_scheduled()

    def test_simple_scheduling_timedout(self):
        self.set_state(timedout=[0, 1])
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.TIMEDOUT, None, 0),
            (self.TIMEDOUT, None, 1)
        )
        self.assert_scheduled()

    def test_simple_scheduling_found(self):
        self.set_state(results={0: 10, 1: 20})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.FOUND, 10, 0),
            (self.FOUND, 20, 1)
        )
        self.assert_scheduled()

    def test_simple_scheduling_error(self):
        self.set_state(errors={0: 'e1', 1: 'e2'})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.ERROR, 'e1', 0),
            (self.ERROR, 'e2', 1)
        )
        self.assert_scheduled()

    def test_schedule_delay(self):
        self.set_state()
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=20)
        self.schedule_activity(spec='a2', input='in3', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
            (self.RUNNING, None, None),
            (self.RUNNING, None, None)
        )
        self.assert_scheduled(
            ('TIMER', 10, 0),  # 0 for timer, 1 for activity
            ('TIMER', 20, 2),
            ('ACTIVITY', 'a2', 4, 'in3'),
        )

    def test_schedule_retry(self):
        self.set_state()
        self.schedule_activity(spec='a1', input='in1', retry=5, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=3, delay=0)
        self.schedule_activity(spec='a2', input='in3', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
            (self.RUNNING, None, None),
            (self.RUNNING, None, None)
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 0, 'in1'),  # 1 for activity + 5 retries
            ('WORKFLOW', 'w1', 6, 'in2'),
            ('ACTIVITY', 'a2', 10, 'in3'),
        )

    def test_schedule_retry_and_delay(self):
        self.set_state()
        self.schedule_activity(spec='a1', input='in1', retry=5, delay=10)
        self.schedule_workflow(spec='w1', input='in2', retry=3, delay=20)
        self.schedule_activity(spec='a2', input='in3', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
            (self.RUNNING, None, None),
            (self.RUNNING, None, None)
        )
        self.assert_scheduled(
            ('TIMER', 10, 0),  # 1 for timer, 1 for activity + 5 retries
            ('TIMER', 20, 7),
            ('ACTIVITY', 'a2', 12, 'in3'),
        )

    # TIMER

    def test_skip_timer_and_reschedule(self):
        self.set_state(results={0: None})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.assert_state(
            (self.RUNNING, None, None),
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 1, 'in1'),  # 0 was the timer
        )

    def test_skip_timer_and_wait(self):
        self.set_state(results={0: None}, running=[1])
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.assert_state(
            (self.RUNNING, None, None),
        )
        self.assert_scheduled()

    def test_skip_timer_and_result(self):
        self.set_state(results={0: None, 1: 10})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.assert_state(
            (self.FOUND, 10, 1),
        )
        self.assert_scheduled()

    def test_skip_timer_and_error(self):
        self.set_state(results={0: None}, errors={1: 'err'})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.assert_state(
            (self.ERROR, 'err', 1),
        )
        self.assert_scheduled()

    def test_skip_timer_and_timeout(self):
        self.set_state(results={0: None}, timedout=[1])
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.assert_state(
            (self.TIMEDOUT, None, 1),
        )
        self.assert_scheduled()

    # TIMEOUT

    def test_skip_timeouts_and_reschedule(self):
        self.set_state(timedout=[0, 1, 2])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 3, 'in1'),  # 0 was the timer
        )

    def test_skip_timeouts_and_wait(self):
        self.set_state(timedout=[0, 1, 2], running=[3])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.RUNNING, None, None),
        )
        self.assert_scheduled()

    def test_skip_timeouts_and_result(self):
        self.set_state(timedout=[0, 1, 2], results={3: 30})
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.FOUND, 30, 3),
        )
        self.assert_scheduled()

    def test_skip_timeouts_and_error(self):
        self.set_state(timedout=[0, 1, 2], errors={3: 'err'})
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.ERROR, 'err', 3),
        )
        self.assert_scheduled()

    def test_skip_timeouts_and_timeout(self):
        self.set_state(timedout=[0, 1, 2, 3])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.TIMEDOUT, None, 3),
        )
        self.assert_scheduled()

    # TIMER and TIMEOUT

    def test_skip_timer_and_timeouts_and_reschedule(self):
        self.set_state(results={0: None}, timedout=[1, 2, 3])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=10)
        self.assert_state(
            (self.RUNNING, None, None),
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 4, 'in1'),  # 0 was the timer
        )

    def test_skip_timer_and_timeouts_and_wait(self):
        self.set_state(results={0: None}, timedout=[1, 2, 3], running=[4])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=10)
        self.assert_state(
            (self.RUNNING, None, None),
        )
        self.assert_scheduled()

    def test_skip_timer_and_timeouts_and_result(self):
        self.set_state(timedout=[1, 2, 3], results={0: None, 4: 40})
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=10)
        self.assert_state(
            (self.FOUND, 40, 4),
        )
        self.assert_scheduled()

    def test_skip_timer_and_timeouts_and_error(self):
        self.set_state(results={0: None}, timedout=[1, 2, 3],
                       errors={4: 'err'})
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=10)
        self.assert_state(
            (self.ERROR, 'err', 4),
        )
        self.assert_scheduled()

    def test_skip_timer_and_timeouts_and_timeout(self):
        self.set_state(results={0: None}, timedout=[1, 2, 3, 4])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=10)
        self.assert_state(
            (self.TIMEDOUT, None, 4),
        )
        self.assert_scheduled()


class TestWorkflowBase(TestCase):
    def set_state(self, running=[], timedout=[], results={}, errors={},
                  order=None):
        self.scheduler = DummyScheduler()
        self.Workflow = self.make_workflow()
        order = list(range(10000))
        self.workflow = self.Workflow(self.scheduler, '[[], {}]', 'token',
                                      running, timedout, results, errors,
                                      order, None, None)
        self.workflow()

    def assert_scheduled(self, *state):
        return self.assertEquals(self.scheduler.state, list(state))


class TestSimpleWorkflow(TestWorkflowBase):

    def make_workflow(self):
        from flowy.task import _SWFWorkflow
        from flowy.proxy import SWFActivityProxy

        class MyWorkflow(_SWFWorkflow):

            a = SWFActivityProxy(name='a', version=1)
            b = SWFActivityProxy(name='b', version=1)
            c = SWFActivityProxy(name='c', version=1)

            def run(self):
                a = self.a('a_input')
                b = self.b('b_input')
                return self.c(a, b)

        return MyWorkflow

    def test_initial_run(self):
        self.set_state()
        self.assert_scheduled(
            ('ACTIVITY', self.Workflow.a._spec, 0, '[["a_input"], {}]'),
            ('ACTIVITY', self.Workflow.b._spec, 4, '[["b_input"], {}]'),
            'FLUSH'
        )

    def test_wait_for_b(self):
        self.set_state(results={0: '1'}, running=[4])
        self.assert_scheduled(
            'FLUSH'
        )

    def test_schedule_c(self):
        self.set_state(results={0: '1', 4: '2'})
        self.assert_scheduled(
            ('ACTIVITY', self.Workflow.c._spec, 8, '[[1, 2], {}]'),
            'FLUSH'
        )

    def test_wait_for_c(self):
        self.set_state(results={0: '1', 4: '2'}, running=[8])
        self.assert_scheduled(
            'FLUSH'
        )

    def test_finish(self):
        self.set_state(results={0: '1', 4: '2', 8: '3'})
        self.assert_scheduled(
            ('COMPLETE', '3'),
        )

    def test_error(self):
        self.set_state(errors={0: 'err'}, running=[4])
        self.assert_scheduled(
            ('FAIL', 'err'),
            'FLUSH'
        )

    def test_return_error(self):
        self.set_state(results={0: '1', 4: '2'}, errors={8: 'err'})
        self.assert_scheduled(
            ('FAIL', 'err'),
            'FLUSH',
        )


class TestErrorBubblingWorkflow(TestWorkflowBase):

    def make_workflow(self):
        from flowy.task import _SWFWorkflow
        from flowy.proxy import SWFActivityProxy
        from flowy.exception import TaskError

        class MyWorkflow(_SWFWorkflow):

            a = SWFActivityProxy(name='a', version=1, error_handling=True)
            b = SWFActivityProxy(name='b', version=1, error_handling=True)
            c = SWFActivityProxy(name='c', version=1, error_handling=True)

            def run(self):
                a = self.a('a_input')
                self.b(a)
                try:
                    a.result()
                except TaskError:
                    return self.c(a)
                return 100

        return MyWorkflow

    def test_error_bubbling(self):
        self.set_state(errors={0: 'err'})
        self.assert_scheduled(
            ('FAIL', 'err'),
        )

    def test_error_silent(self):
        self.set_state(results={0: '0'}, errors={4: 'err'})
        self.assert_scheduled(
            ('COMPLETE', '100'),
        )


class TestExceptionInWorkflow(TestWorkflowBase):

    def make_workflow(self):
        from flowy.task import _SWFWorkflow

        class MyWorkflow(_SWFWorkflow):

            def run(self):
                raise ValueError('err')

        return MyWorkflow

    def test_error_bubbling(self):
        self.set_state()
        self.assert_scheduled(
            ('FAIL', 'err'),
        )


class TestResultBlocksWorkflow(TestWorkflowBase):

    def make_workflow(self):
        from flowy.task import _SWFWorkflow
        from flowy.proxy import SWFActivityProxy

        class MyWorkflow(_SWFWorkflow):

            a = SWFActivityProxy(name='a', version=1)
            b = SWFActivityProxy(name='b', version=1)

            def run(self):
                a = self.a('a_input')
                a.result()
                self.b('b_input')

        return MyWorkflow

    def test_a_blocks_when_scheduled(self):
        self.set_state()
        self.assert_scheduled(
            ('ACTIVITY', self.Workflow.a._spec, 0, '[["a_input"], {}]'),
            'FLUSH'
        )

    def test_a_blocks_when_running(self):
        self.set_state(running=[0])
        self.assert_scheduled(
            'FLUSH'
        )

    def test_a_doesnt_block_when_finished(self):
        self.set_state(results={0: '0'})
        self.assert_scheduled(
            ('ACTIVITY', self.Workflow.b._spec, 4, '[["b_input"], {}]'),
            'FLUSH'
        )
