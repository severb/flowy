from boto.swf.exceptions import SWFResponseError

from flowy import MagicBind
from flowy.runtime import OptionsRuntime
from flowy.swf import SWFTaskId
from flowy.swf.runtime import ActivityRuntime, DecisionRuntime


class ActivityPollerClient(object):
    def __init__(self, client, task_list, runtime_factory=ActivityRuntime):
        self._client = client
        self._task_list = task_list
        self._runtime_factory = runtime_factory

    def poll_next_task(self, poller):
        task = None
        while task is None:
            swf_response = self._poll_response()
            task_id, input, token = self._parse_response(swf_response)
            runtime = self._runtime_factory(
                client=MagicBind(self._client, token=token)
            )
            task = poller.make_task(
                task_id=task_id,
                input=input,
                runtime=runtime
            )
        return task

    def _parse_response(self, swf_response):
        return (
            SWFTaskId(
                swf_response['activityType']['name'],
                swf_response['activityType']['version']
            ),
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


def decision_runtime(client, events):
    return OptionsRuntime(DecisionRuntime(client, events))


class DecisionPollerClient(object):
    def __init__(self, client, task_list, runtime_factory=decision_runtime):
        self._client = client
        self._task_list = task_list
        self._runtime_factory = runtime_factory

    def poll_next_task(self, poller):
        task = None
        while task is None:
            swf_response = self._poll_response()
            task_id, input, token, events = self._parse_response(swf_response)
            runtime = self._runtime_factory(
                client=MagicBind(self._client, token=token), events=events
            )
            task = poller.make_task(
                task_id=task_id,
                input=input,
                runtime=runtime
            )
        return task

    def _parse_response(self, swf_response):
        pass

    def _poll_response(self):
        swf_response = {}
        while 'taskToken' not in swf_response or not swf_response['taskToken']:
            try:
                swf_response = self._client.poll_for_decision_task(
                    task_list=self._task_list
                )
            except SWFResponseError:
                pass
        return swf_response
