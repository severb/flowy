from unittest import TestCase


class DummyScheduler(object):

    def __init__(self):
        self.state = []

    def flush(self):
        self.state.append('FLUSH')

    def restart(self):
        self.state.append('RESTART')

    def fail(self, reason):
        self.state.append(('FAIL', reason))

    def complete(self, result):
        self.state.append(('COMPLETE', result))

    def schedule_timer(self, delay, call_id):
        self.state.append(('TIMER', delay, call_id))

    def schedule_activity(self, spec, call_id, input):
        self.state.append(('ACTIVITY', spec, call_id, input))

    def schedule_workflow(self, spec, call_id, input):
        self.state.append(('WORKFLOW', spec, call_id, input))


class TestWorkflowScheduling(TestCase):

    def set_state(self, running=[], timedout=[], results={}, errors={}):
        from flowy.task import _SWFWorkflow
        self.scheduler = DummyScheduler()
        self.workflow = _SWFWorkflow(self.scheduler, 'input', 'token', running,
                                     timedout, results, errors, None, None)
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
            (self.RUNNING, None),
            (self.RUNNING, None)
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
            (self.RUNNING, None),
            (self.RUNNING, None)
        )
        self.assert_scheduled()

    def test_simple_scheduling_timedout(self):
        self.set_state(timedout=[0, 1])
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.TIMEDOUT, None),
            (self.TIMEDOUT, None)
        )
        self.assert_scheduled()

    def test_simple_scheduling_found(self):
        self.set_state(results={0: 10, 1: 20})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.FOUND, 10),
            (self.FOUND, 20)
        )
        self.assert_scheduled()

    def test_simple_scheduling_error(self):
        self.set_state(errors={0: 'e1', 1: 'e2'})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=0)
        self.assert_state(
            (self.ERROR, 'e1'),
            (self.ERROR, 'e2')
        )
        self.assert_scheduled()

    def test_schedule_delay(self):
        self.set_state()
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.schedule_workflow(spec='w1', input='in2', retry=0, delay=20)
        self.schedule_activity(spec='a2', input='in3', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None),
            (self.RUNNING, None),
            (self.RUNNING, None)
        )
        self.assert_scheduled(
            ('TIMER', 10, 0), # 0 for timer, 1 for activity
            ('TIMER', 20, 2),
            ('ACTIVITY', 'a2', 4, 'in3'),
        )

    def test_schedule_retry(self):
        self.set_state()
        self.schedule_activity(spec='a1', input='in1', retry=5, delay=0)
        self.schedule_workflow(spec='w1', input='in2', retry=3, delay=0)
        self.schedule_activity(spec='a2', input='in3', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None),
            (self.RUNNING, None),
            (self.RUNNING, None)
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 0, 'in1'), # 1 for activity + 5 retries
            ('WORKFLOW', 'w1', 6, 'in2'),
            ('ACTIVITY', 'a2', 10, 'in3'),
        )

    def test_schedule_retry_and_delay(self):
        self.set_state()
        self.schedule_activity(spec='a1', input='in1', retry=5, delay=10)
        self.schedule_workflow(spec='w1', input='in2', retry=3, delay=20)
        self.schedule_activity(spec='a2', input='in3', retry=0, delay=0)
        self.assert_state(
            (self.RUNNING, None),
            (self.RUNNING, None),
            (self.RUNNING, None)
        )
        self.assert_scheduled(
            ('TIMER', 10, 0), # 1 for timer, 1 for activity + 5 retries
            ('TIMER', 20, 7),
            ('ACTIVITY', 'a2', 12, 'in3'),
        )

    # TIMER

    def test_skip_timer_and_reschedule(self):
        self.set_state(results={0: None})
        self.schedule_activity(spec='a1', input='in1', retry=0, delay=10)
        self.assert_state(
            (self.RUNNING, None),
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 1, 'in1'), # 0 was the timer
        )

    def test_skip_timer_and_wait(self):
        self.set_state(results={0: None}, running=[1])
        self.schedule_activity(spec='a1', input='in1', retry=2, delay=10)
        self.assert_state(
            (self.RUNNING, None),
        )
        self.assert_scheduled()

    def test_skip_timer_and_result(self):
        self.set_state(results={0: None, 1: 10})
        self.schedule_activity(spec='a1', input='in1', retry=2, delay=10)
        self.assert_state(
            (self.FOUND, 10),
        )
        self.assert_scheduled()

    def test_skip_timer_and_error(self):
        self.set_state(results={0: None}, errors={1: 'err'})
        self.schedule_activity(spec='a1', input='in1', retry=2, delay=10)
        self.assert_state(
            (self.ERROR, 'err'),
        )
        self.assert_scheduled()

    def test_skip_timer_and_timeout(self):
        self.set_state(results={0: None}, timedout=[1, 2, 3])
        self.schedule_activity(spec='a1', input='in1', retry=2, delay=10)
        self.assert_state(
            (self.TIMEDOUT, None),
        )
        self.assert_scheduled()


    # TIMEOUT

    def test_skip_timeouts_and_reschedule(self):
        self.set_state(timedout=[0, 1, 2])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.RUNNING, None),
        )
        self.assert_scheduled(
            ('ACTIVITY', 'a1', 3, 'in1'), # 0 was the timer
        )

    def test_skip_timeouts_and_wait(self):
        self.set_state(timedout=[0, 1, 2], running=[3])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.RUNNING, None),
        )
        self.assert_scheduled()

    def test_skip_timeouts_and_result(self):
        self.set_state(timedout=[0, 1, 2], results={3: 30})
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.FOUND, 30),
        )
        self.assert_scheduled()

    def test_skip_timeouts_and_error(self):
        self.set_state(timedout=[0, 1, 2], errors={3: 'err'})
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.ERROR, 'err'),
        )
        self.assert_scheduled()

    def test_skip_timeouts_and_timeout(self):
        self.set_state(timedout=[0, 1, 2, 3])
        self.schedule_activity(spec='a1', input='in1', retry=3, delay=0)
        self.assert_state(
            (self.TIMEDOUT, None),
        )
        self.assert_scheduled()
