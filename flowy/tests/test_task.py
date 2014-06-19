import uuid
from unittest import TestCase


class TestWorkflowScheduling(TestCase):

    def prepare(self, running=[], timedout=[], results={}, errors={}):
        from flowy.task import SWFWorkflow
        token = str(uuid.uuid4())
        self.workflow_task = SWFWorkflow(None, None, token, running, timedout,
                                         results, errors, None, None)
        self._TIMEDOUT = self.workflow_task._TIMEDOUT
        self._RUNNING = self.workflow_task._RUNNING
        self._ERROR = self.workflow_task._ERROR
        self._FOUND = self.workflow_task._FOUND
        self.states = []
        self.call_count = 0

    def schedule_activity(self, retry=0, delay=0):
        from flowy.spec import SWFActivitySpec
        spec = SWFActivitySpec('dummy', 1)
        input = 'i_%s' % self.call_count
        self.call_count += 1
        s, v = self.workflow_task.schedule_activity(spec, input, retry, delay)
        self.states.append((s, v))

    def assert_states(self, *states):
        self.assertEquals(list(states), self.states)

    def assert_scheduled(self, *ids):
        scheduled_ids = []
        for s in self.workflow_task._decisions._data:
            id = int(s['scheduleActivityTaskDecisionAttributes']['activityId'])
            scheduled_ids.append(id)
        self.assertEquals(list(ids), scheduled_ids)

    def test_simple_schedule(self):
        self.prepare(running=[], timedout=[], results={}, errors={})
        self.schedule_activity(retry=0, delay=0)
        self.schedule_activity(retry=0, delay=0)
        self.schedule_activity(retry=0, delay=0)
        self.assert_states(
            (self._RUNNING, None),
            (self._RUNNING, None),
            (self._RUNNING, None)
        )
        self.assert_scheduled(0, 1, 2)
