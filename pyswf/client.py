import json
from boto.swf.layer1 import Layer1


# job_interface:
# id
# __call__(context)

# scheduled_job
# id
# args
# kwargs
# token

class BaseClient(object):
    def __init__(self, jobs, client=None):
        self.job_runners = {}
        for job in jobs:
            self.register_job_runner(job)
        self.client = client is not None and client or Layer1()

    def register_job_runner(self, job):
        self.job_runners[job.id] = job

    def select_job_runner(self, job_id):
        return self.job_runners[job_id]

    def run(self, domain, task_lits):
        while 1:
            self.process_next_job(domain, task_list)

    def process_next_job(self, domain, task_list):
        context, scheduled_job = self.poll_next_job(domain, task_list)
        Runner = self.select_job_runner(scheduled_job.id)
        job_runner = Runner(context)
        job_result = job_runner(*scheduled_job.args, **scheduled_job.kwargs)
        self.save_result(scheduled_job.token, job_result)

    def poll_next_job(self, domain, task_list):
        raise NotImplemented()

    def save_result(self, job_result):
        raise NotImplemented()


class ScheduledWorkflow(object):
    def __init__(self, response):
        self.id = ('a', '1')
        self.args = []
        self.kwargs = {}
        self.token = '123'


class WorkflowClient(BaseClient):
    def register_job_runner(self, workflow):
        super(self, WorkflowClient).register_job_runner(workflow)
        # XXX: check if the workflow is registered

    def poll_next_job(self, domain, task_list):
        response = self.client.poll_for_decision_task(domain, task_list)
        return WorkflowHistory(response), ScheduledWorkflow(response)

    def save_result(self, workflow_token, scheduled_tasks):
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


class ScheduledActivity(object):
    def __init__(self, response):
        self.id = ('a', '1')
        self.args = []
        self.kwargs = {}
        self.token = '123'


class ActivityClient(BaseClient):
    def register_job_runner(self, activity):
        super(self, WorkflowClient).register_job_runner(workflow)
        # XXX: check if the activity is registered

    def poll_next_job(self, domain, task_list):
        response = self.client.poll_for_activity_task(domain, task_list)
        return None, ScheduledActivity(response)

    def save_result(self, activity_token, activity_result):
        self.client.respond_activity_task_completed(activity_token, activity_result)


class SWFClient(object):
    def __init__(self, workflows=[], activities=[], client=None):
        self.workflow_client = WorkflowClient(workflows, client)
        self.activity_client = WorkflowClient(activities, client)

    def run(self):
        while 1:
            self.workflow_client.process_next_job()
            self.activity_client.process_next_job()

    def register_activity_runner(self, activity):
        self.activity_client.register_job_runner(activity)

    def register_workflow_runner(self, workflow):
        self.workflow_client.register_job_runner(workflow)


class ThreadedSWFClient(SWFClient):
    def run(self):
        pass
