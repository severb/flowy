import json
from functools import partial


class Remote(object):
    def __init__(self, decision_task, result):
        self._decision_task = decision_task
        self._result = result

    def __getattr__(self, proxy_name):
        proxy = getattr(self._decision_task, proxy_name)
        if not callable(proxy):
            raise AttributeError('%r is not callable' % proxy_name)
        return partial(proxy, self)

    def options(self):
        pass

    def call_remote_activity(self):
        pass

    def call_remote_subworkflow(self):
        pass


class ActivityTask(object):
    def __init__(self, input, result):
        self._input = input
        self._result = result

    def __call__(self):
        try:
            args, kwargs = self.deserialize_arguments()
            result = self._call_run(*args, **kwargs)
        except Exception as e:
            self._result.fail(str(e))
        else:
            self._result.complete(self.serialize_result(result))

    def _call_run(self, *args, **kwargs):
        self.run(self._result.heartbeat, *args, **kwargs)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def serialize_result(self, result):
        return json.dumps(result)

    def deserialize_arguments(self):
        return json.loads(self._input)


class DecisionTask(ActivityTask):
    def __init__(self, input, result, remote=Remote):
        super(DecisionTask, self).__init__(input, result)
        self._remote_factory = remote

    def _call_run(self, *args, **kwargs):
        self.run(self._remote_factory(self, self._result), *args, **kwargs)
