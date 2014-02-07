class TaskSpec(object):
    def __init__(self, task_id, task_factory, *args, **kwargs):
        self._task_id = task_id
        self._task_factory = task_factory

    def register(self, worker):
        worker.register(
            task_id=self._task_id,
            task_factory=self._task_factory
        )

    def __repr__(self):
        return '%s(%r, %r)' % (
            self.__class__.__name__, self._task_id, self._task_factory
        )


class RemoteTaskSpec(TaskSpec):
    def __init__(self, task_id, task_factory, client):
        super(RemoteTaskSpec, self).__init__(task_id, task_factory)
        self._client = client

    def register(self, worker=None):
        successfuly_registered_remote = self._register_remote()
        if not successfuly_registered_remote:
            return False
        if worker is not None:
            super(RemoteTaskSpec, self).register(worker)
        return True

    def _register_remote(self):
        success = True
        registered_as_new = self._try_register_remote()
        if not registered_as_new:
            success = self._check_if_compatible()
        return success

    def _try_register_remote(self):
        raise NotImplementedError

    def _check_if_compatible(self):
        raise NotImplementedError


class ActivitySpecCollector(object):
    def __init__(self, spec_factory, client):
        self._spec_factory = spec_factory
        self._client = client
        self._specs = []

    def register(self, worker=None):
        unregistered_spec = []
        for s in self._specs:
            if not s.register(worker):
                unregistered_spec.append(s)
        return unregistered_spec

    def collect(self, task_id, task_factory, task_list,
                heartbeat, schedule_to_close,
                schedule_to_start, start_to_close):
        self._specs.append(self._spec_factory(
            task_id=task_id,
            task_factory=task_factory,
            client=self._client,
            task_list=task_list,
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close
        ))


class WorkflowSpecCollector(ActivitySpecCollector):
    def collect(self, task_id, task_factory, task_list,
                workflow_duration, decision_duration):
        self._specs.append(self._spec_factory(
            task_id=task_id,
            task_factory=task_factory,
            client=self._client,
            task_list=task_list,
            workflow_duration=workflow_duration,
            decision_duration=decision_duration
        ))
