from zope.interface import implementer

from boto.swf.layer1 import Layer1
from boto.swf.exceptions import SWFTypeAlreadyExistsError

from pyswf.interface import IWorkflowClient


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
        activity_task = ActivityTask(response)
        return ActivityContext(activity_task)

    def save(self, token, activity_result):
        self.client.respond_activity_task_completed(token, activity_result)


@implementer(IWorkflowClient)
class WorkflowClient(object):
    def __init__(self, domain, task_list, client=None):
        self.client = client if client is not None else Layer1()
        self.domain = domain
        self.task_list = task_list
        self.workflows = {}

    def register(self, name, version, workflow_runner,
        execution_start_to_close=3600,
        task_start_to_close=60,
        child_policy='TERMINATE',
        doc=None
    ):
        self.workflows[(name, version)] = workflow_runner
        try:
            self.client.register_workflow_type(
                self.domain,
                name,
                version,
                self.task_list,
                child_policy,
                execution_start_to_close,
                task_start_to_close,
                doc
            )
        except SWFTypeAlreadyExistsError:
            pass # Check if the registered workflow has the same properties.

    def schedule_activities(self, token, activities):
        pass

    def complete_workflow(self, token, result):
        pass

    def run(self):
        while 1:
            response = self._poll()
            context = IWorkflowContext(response.context)
            for event in response.events:
                event.update(context)
            runner = self._query(response.name, response.version)
            runner.resume(response, context)

    def _poll(self, domain, task_list):
        response = self.client.poll_for_decision_task(
            self.domain, self.task_list
        )
        return IWorflowResponse(response, self.client)

    def _query(self, name, version):
        return self.workflows[(name, version)]


@implementer(IWorflowResponse)
class WorkflowResponse(object):
    def __init__(self, api_response, client):
        self.api_response = api_response
        self.scheduled = []

    @property
    def name(self):
        return self.api_response['workflowType']['name']

    @property
    def version(self):
        return self.api_response['workflowType']['version']

    @property
    def context(self):
        pass

    def schedule(self, activity_name, activity_version, input):
        self.scheduled.append((activity_name, activity_version, input))

    def suspend(self, context):
        pass

    def complete(self, result):
        pass

    def __iter__(self):
        pass

    def _events(self):
        api_response = self.api_response
        for event in api_response['events']:
            yield event
        while api_response['nextPageToken']:
            api_response = self.client.poll_for_decision_task(
                domain, task_list,
            )
            for event in api_response['events']:
                yield event
