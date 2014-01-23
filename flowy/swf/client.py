from boto.swf.exceptions import SWFResponseError

from flowy.swf.runtime import ActivityResult, Heartbeat
from flowy.swf import SWFTaskId


class ActivityPollerClient(object):
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
                SWFTaskId(name, version),
                input=input,
                result=activity_result,
                task_runtime=heartbeat
            )
        return task

    def _parse_response(self, swf_response):
        return (
            swf_response['activityType']['name'],
            swf_response['activityType']['version'],
            swf_response['input'],
            swf_response['taskToken']
        )

    def _poll_response(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._client.poll_for_activity_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                pass  # Add a delay before retrying?
        return swf_response
