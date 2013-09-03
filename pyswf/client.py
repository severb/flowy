import json
from boto.swf.layer1 import Layer1

class SWFClient(object):
    def __init__(self, workflows=[], activities=[], client=None):
        self.workflows = dict(((w.name, w.version), w) for w in workflows)
        self.activities = dict(((a.name, a.version), a) for a in activities)
        self.client = client is not None and client or Layer1()

    def run(self, domain, task_list):
        while 1:
            if self.workflows:
                history = self.poll_next_decision(domain, task_list)
                Workflow = self.select_workflow(history)
                w = Workflow(history)
                w(*history.input_as_args, **history.input_as_kwargs)
                decisions = self.as_decisions(w._scheduled)
                self.client.respond_decision_task_completed(
                    history.task_token,
                    decisions=decisions
                )
            if self.activities:
                activity_spec = self.poll_next_activity(domain, task_list)
                activity = self.select_activity(actvitiy_spec)
                result = activity(
                    *activity_spec.input_as_args,
                    **activity_spec.input_as_kwargs
                )
                self.client.respond_activity_task_completed(
                    activity_spec.task_token,
                    json.dumps(result)
                )

    def poll_next_decision(self, domain, task_list):
        h = self.client.poll_for_decision_task(domain, task_list)
        return History(h)

    def as_decisions(self, scheduled):
        l = Layer1Decisions()
        for invocation_id, activity, input in scheduled:
            l.schedule_activity_task(
                invocation_id,
                activity.name,
                activity.version,
                input = input
            )
        return l._data

    def select_workflow(self, history):
        return self.workflows[
            (history.workflow_name, history.workflow_version)
        ]
