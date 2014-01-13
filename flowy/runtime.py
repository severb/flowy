from functools import partial


class DecisionRuntime(object):
    def __init__(self, decision_task, client):
        self._decision_task = decision_task
        self._client = client

    def __getattr__(self, proxy_name):
        proxy = getattr(self._decision_task, proxy_name)
        if not callable(proxy):
            raise AttributeError('%r is not callable' % proxy_name)
        return partial(proxy, self)

    def remote_activity(self, heartbeat, result_deserializer):
        pass

    def remote_subworkflow(self, heartbeat, result_deserializer):
        pass

    def options(self, heartbeat):
        pass


class Heartbeat(object):
    def __init__(self, client, token):
        self._client = client
        self._token = token

    def __call__(self):
        pass
