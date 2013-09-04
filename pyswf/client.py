from boto.swf.layer1 import Layer1


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
        job_context = self.poll_next_job(domain, task_list)
        job_runner = self.select_job_runner(job_context.id)
        result = job_context.execute(self.client, job_runner)
        self.save(job_context.token, result)

    def poll_next_job(self, domain, task_list):
        raise NotImplemented()

    def save(self, job_result):
        raise NotImplemented()


class WorkflowClient(BaseClient):
    def register_job_runner(self, workflow):
        super(self, WorkflowClient).register_job_runner(workflow)
        # XXX: check if the workflow is registered

    def poll_next_job(self, domain, task_list):
        response = self.client.poll_for_decision_task(domain, task_list)
        return WorkflowContext(response)

    def save(self, token, scheduled_activities):
        l = Layer1Decisions()
        for invocation_id, activity, input in scheduled:
            l.schedule_activity_task(
                invocation_id,
                activity.name,
                activity.version,
                input=input
            )
        self.client.respond_decision_task_completed(token, decisions=l._data)


class ActivityClient(BaseClient):
    def register_job_runner(self, activity):
        super(self, WorkflowClient).register_job_runner(workflow)
        # XXX: check if the activity is registered

    def poll_next_job(self, domain, task_list):
        response = self.client.poll_for_activity_task(domain, task_list)
        return ActivityContext(response)

    def save(self, token, activity_result):
        self.client.respond_activity_task_completed(token, activity_result)


class DevSWFClient(object):
    def __init__(self, workflows=[], activities=[], client=None):
        self.workflow_client = WorkflowClient(workflows, client)
        self.activity_client = WorkflowClient(activities, client)

    def run(self):
        # XXX: run with multiple threads
        while 1:
            self.workflow_client.process_next_job()
            self.activity_client.process_next_job()

    def register_activity_runner(self, activity):
        self.activity_client.register_job_runner(activity)

    def register_workflow_runner(self, workflow):
        self.workflow_client.register_job_runner(workflow)
