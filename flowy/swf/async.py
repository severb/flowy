from boto.swf.layer1 import Layer1
from flowy.swf.scheduler import ActivityScheduler
from flowy.task import serialize_result


class AsyncScheduler(object):

    def __init__(self, domain, layer1=None, scheduler=ActivityScheduler):
        self._client = layer1
        if layer1 is None:
            self._client = Layer1()
        self._scheduler = scheduler

    def finish_activity(self, token, result):
        scheduler = self._scheduler(self._client, token)
        return scheduler.finish(self._serialize_result(result))

    _serialize_result = serialize_result
