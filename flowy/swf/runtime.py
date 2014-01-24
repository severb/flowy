from boto.swf.exceptions import SWFResponseError


class ActivityRuntime(object):
    def __init__(self, client):
        self._client = client

    def heartbeat(self):
        try:
            self._client.record_activity_task_heartbeat()
        except SWFResponseError:
            return False
        return True

    def complete(self, result):
        try:
            self._client.respond_activity_task_completed(result=result)
        except SWFResponseError:
            return False
        return True

    def fail(self, reason):
        try:
            self._client.respond_activity_task_failed(reason=reason)
        except SWFResponseError:
            return False
        return True

    def suspend(self):
        pass


class DecisionRuntime(object):
    def __init__(self, client):
        self._client = client

    def remote_activity(self, result_deserializer,
                        heartbeat=None,
                        schedule_to_close=None,
                        schedule_to_start=None,
                        start_to_close=None,
                        task_list=None,
                        retry=None,
                        delay=None,
                        error_handling=None):
        pass

    def remote_subworkflow(self, result_deserializer,
                           heartbeat=None,
                           workflow_duration=None,
                           decision_duration=None,
                           task_list=None,
                           retry=3,
                           delay=0,
                           error_handling=None):
        pass

    def complete(self, result):
        pass

    def fail(self, reason):
        pass

    def suspend(self):
        pass
