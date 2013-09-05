from boto.swf.layer1 import Layer1
from boto.swf.layer1_decisions import Layer1Decisions
from boto.swf.exceptions import SWFTypeAlreadyExistsError

from pyswf.context import WorkflowContext, ActivityContext


class BaseClient(object):
    def __init__(self, domain, task_list, jobs, client=None):
        self.domain = domain
        self.task_list = task_list
        self.client = client is not None and client or Layer1()
        self.job_runners = {}
        for job in jobs:
            self.register_job_runner(job)

    def register_job_runner(self, job):
        self.job_runners[(job.name, str(job.version))] = job

    def select_job_runner(self, job):
        return self.job_runners[(job.name, job.version)]

    def run(self):
        while 1:
            self.process_next_job()

    def process_next_job(self):
        job_context = self.poll_next_job()
        if job_context.is_empty():
            return
        job_runner = self.select_job_runner(job_context)
        result = job_context.execute(job_runner)
        self.save(job_context.token, result)

    def poll_next_job(self, domain, task_list):
        raise NotImplemented()

    def save(self, job_result):
        raise NotImplemented()


class WorkflowClient(BaseClient):
    def register_job_runner(self, workflow):
        super(WorkflowClient, self).register_job_runner(workflow)
        try:
            self.client.register_workflow_type(
                self.domain,
                workflow.name,
                str(workflow.version),
                self.task_list,
                workflow.child_policy,
                str(workflow.execution_start_to_close),
                str(workflow.task_start_to_close),
                workflow.__doc__
            )
        except SWFTypeAlreadyExistsError:
            pass # Check if the registered workflow has the same properties.

    def poll_next_job(self):
        response = self.client.poll_for_decision_task(
            self.domain, self.task_list
        )
        return WorkflowContext(response)

    def save(self, token, (scheduled, still_running, result)):
        l = Layer1Decisions()
        if scheduled or still_running:
            for invocation_id, activity, input in scheduled:
                l.schedule_activity_task(
                    invocation_id,
                    activity.name,
                    str(activity.version),
                    input=input
                )
        else:
            l.complete_workflow_execution(result=result)
        self.client.respond_decision_task_completed(token, decisions=l._data)


class ActivityClient(BaseClient):
    def register_job_runner(self, activity):
        super(ActivityClient, self).register_job_runner(activity)
        try:
            self.client.register_activity_type(
                self.domain,
                activity.name,
                str(activity.version),
                self.task_list,
                str(activity.heartbeat),
                str(activity.schedule_to_close),
                str(activity.schedule_to_start),
                str(activity.task_start_to_close),
                activity.__doc__
            )
        except SWFTypeAlreadyExistsError:
            pass # Check if the registered activity has the same properties.

    def poll_next_job(self):
        response = self.client.poll_for_activity_task(
            self.domain, self.task_list
        )
        return ActivityContext(response)

    def save(self, token, activity_result):
        self.client.respond_activity_task_completed(token, activity_result)


class SWFClient(object):
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
