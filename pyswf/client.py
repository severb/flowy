import json
from boto.swf.layer1 import Layer1

class SWFClient(object):
    def __init__(self, workflows=[], activities=[], client=None):
        self.workflows = dict((w.id, w) for w in workflows)
        self.activities = dict((a.id, a) for a in activities)
        self.client = client is not None and client or Layer1()

    def run(self, domain, task_list):
        while 1:
            if self.workflows:
                self.advance_next_workflow(domain, task_list)
            if self.activities:
                self.run_next_activity(domain, task_list)

    def advance_next_workflow(self, domain, task_list):
        response = self.poll_next_decision(domain, task_list)
        wf_history = WorkflowHistory(response)
        Workflow = self.select_workflow(wf_history.id)
        workflow_runner = Workflow(wf_history)
        args = self.input_to_args(wf_history.input)
        kwargs = self.input_to_kwargs(wf_history.input)
        scheduled = workflow_runner(*args, **kwargs)
        self.send_decision_results(wf_history, scheduled)

    def poll_next_decision(self, domain, task_list):
        return self.client.poll_for_decision_task(domain, task_list)

    def select_workflow(self, workflow_id):
        return self.workflows[workflow_id]

    def send_decision_results(self, wf_history, scheduled):
        l = Layer1Decisions()
        for invocation_id, activity, args, kwargs in scheduled:
            l.schedule_activity_task(
                invocation_id,
                activity.name,
                activity.version,
                input = self.serialize_input(*args, **kwargs)
            )
        self.client.respond_decision_task_completed(
            wf_history.task_token,
            decisions=l._data
        )

    def run_next_activity(self, domain, task_list):
        response = self.poll_next_activity(domain, task_list)
        activity_spec = ActivitySpecification(response)
        activity_runner = self.select_activity(actvitiy_spec.id)
        args = self.input_to_args(activity_spec.input)
        kwagrs = self.input_to_kwargs(activity_spec.input)
        result = activity(*args, **kwargs)
        self.send_activity_result(activity_spec, result)

    def poll_next_activity(self, domain, task_list):
        return self.client.poll_for_activity_task(domain, task_list)

    def select_activity(self, activity_spec_id):
        return self.activities[activity_spec_id]

    def send_activity_result(self, activity_spec, result):
        self.client.respond_activity_task_completed(
            activity_spec.task_token,
            self.serialize_result(result)
        )

    def input_to_args(self, input):
        return json.loads(input)['args']

    def input_to_kwargs(self, input):
        return json.loads(input)['kwargs']

    def serialize_input(self, *args, **kwargs):
        return json.dumps({
            'args': args,
            'kwargs': kwargs
        })

    def serialize_result(self, result):
        return json.dumps({
            'error': False,
            'context': result
        })
