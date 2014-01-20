from boto.swf.exceptions import SWFResponseError

from flowy.runtime import ActivityResult, Heartbeat


class SWFDomainBoundClient(object):
    def __init__(self, client, domain):
        self._client = client
        self._domain = domain

    def poll_for_decision_task(self, task_list, identity=None,
                               maximum_page_size=None,
                               next_page_token=None,
                               reverse_order=None):
        return self._client.poll_for_decision_task(
            self._domain,
            task_list,
            identity,
            maximum_page_size,
            next_page_token,
            reverse_order
        )

    def poll_for_activity_task(self, task_list, identity=None):
        return self._client.poll_for_activity_task(
            self._domain, task_list, identity
        )

    def register_workflow_type(self, name, version,
                               task_list=None,
                               default_child_policy=None,
                               default_execution_start_to_close_timeout=None,
                               default_task_start_to_close_timeout=None,
                               description=None):
        return self._client.register_workflow_type(
            self._domain,
            name,
            version,
            task_list,
            default_child_policy,
            default_execution_start_to_close_timeout,
            default_task_start_to_close_timeout,
            description
        )

    def register_activity_type(self, name, version, task_list=None,
                               default_task_heartbeat_timeout=None,
                               default_task_schedule_to_close_timeout=None,
                               default_task_schedule_to_start_timeout=None,
                               default_task_start_to_close_timeout=None,
                               description=None):
        return self._client.register_activity_type(
            self._domain,
            name,
            version,
            task_list,
            default_task_heartbeat_timeout,
            default_task_schedule_to_close_timeout,
            default_task_schedule_to_start_timeout,
            default_task_start_to_close_timeout,
            description
        )

    def describe_activity_type(self, activity_name, activity_version):
        return self._client.describe_activity_type(
            self._domain,
            activity_name,
            activity_version
        )


class SWFActivityPollerClient(object):
    def __init__(self, client, task_list,
                 result_factory=ActivityResult,
                 runtime_factory=Heartbeat):
        self._client = client
        self._result_factory = result_factory
        self._runtime_factory = runtime_factory
        self._task_list = task_list

    def poll_next_task(self, poller):
        task = None
        while task is None:
            swf_response = self._poll_response()
            name, version, input, token = self._parse_response(swf_response)
            heartbeat = self._runtime_factory(
                client=self._client, token=token
            )
            activity_result = self._result_factory(
                client=self._client, token=token
            )
            task = poller.make_task(
                name=name,
                version=version,
                input=input,
                heartbeat=heartbeat,
                task_result=activity_result
            )
        return task

    def _parse_response(self, swf_response):
        return (
            swf_response['workflowType']['name'],
            swf_response['workflowType']['version'],
            swf_response['input'],
            swf_response['taskToken']
        )

    def _poll_response(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._client.poll_for_activity_task(
                    task_list=self._task_list,
                )
            except SWFResponseError:
                pass  # Add a delay before retrying?
