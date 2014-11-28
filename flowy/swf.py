def activity_fail(self, reason):
    try:
        self._swf_client.respond_activity_task_failed(
            reason=str(reason)[:256], task_token=str(self._token))
    except SWFResponseError:
        logger.exception('Error while failing the activity:')
        return False
    return True


def activity_finish(self, result):
    try:
        result = self._serialize_result(result)
    except Exception as e:
        logger.exception('Error while serializing the result:')
        return activity_fail(self, e)
    try:
        self._swf_client.respond_activity_task_completed(
            result=str(result), task_token=str(self._token))
    except SWFResponseError:
        logger.exception('Error while finishing the activity:')
        return False
    return True


def activity_heartbeat(self):
    try:
        t = str(self._token)
        self._swf_client.record_activity_task_heartbeat(task_token=t)
    except SWFResponseError:
        logger.exception('Error while sending the heartbeat:')
        return False
    return True


class SWFActivity(Task):

    heartbeat = activity_heartbeat
    _fail = activity_fail
    _finish = activity_finish

    def __init__(self, swf_client, input, token):
        self._swf_client = swf_client
        self._token = token
        super(SWFActivity, self).__init__(input, token)

    def _flush(self):
        pass


class SWFScheduler(object):
    def __init__(self, swf_client, token, rate_limit=64):
        self._swf_client = swf_client
        self._token = token
        self._rate_limit = rate_limit
        self._decisions = Layer1Decisions()
        self._closed = False

    def flush(self):
        if self._closed:
            raise RuntimeError('The scheduler is already flushed.')
        self._closed = True
        try:
            self._swf_client.respond_decision_task_completed(
                task_token=self._token, decisions=self._decisions._data
            )
        except SWFResponseError:
            logger.exception('Error while sending the decisions:')

    def reset(self):
        self._decisions = Layer1Decisions()

    def restart(self, spec, input, tags):
        spec.restart(self._decisions, input, tags)

    def fail(self, reason):
        decisions.fail_workflow_execution(reason=str(reason)[:256])

    def complete(self, result):
        self._decisions.complete_workflow_execution(str(result))

    def schedule(self, proxy, call_key, a, kw, delay):
        if len(self._decisions._data) > self._rate_limit:
            return
        delay = int(delay)
        if max(delay, 0):
            self._decisions.start_timer(
                start_to_fire_timeout=str(delay),
                timer_id=str('%s:timer' % call_ke)
            )
        else:
            proxy.schedule(self._decisions, call_key, a, kw)


class SWFWorkflow(Workflow):
    def __init__(self, swf_client, input, token, running, timedout, results,
                 errors, order, spec, tags):
        s = SWFScheduler(swf_client, token, rate_limit=64 - len(running))
        self._tags = tags
        super(SWFWorkflow, self).__init__(s, input, running, timedout,
                                          results, errors, order, spec)

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, tags=_sentinel):
        old_tags = self._tags
        if tags is not _sentinel:
            self._tags = tags
        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            yield
        self._tags = old_tags

    def restart(self, *args, **kwargs):
        try:
            input = self._serialize_restart_arguments(*args, **kwargs)
        except Exception as e:
            logger.exception('Error while serializing restart arguments:')
            self._fail(e)
        else:
            self._scheduler.reset()
            self._scheduler.restart(self._spec, input, self._tags)
        self._scheduled = []
        raise SuspendTask


class SWFActivityProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, retry=[0, 0, 0], error_handling=False,
                 serialize_arguments=serialize_arguments,
                 deserialize_result=deserialize_result):
        self._spec = SWFActivitySpec(name, version, task_list, heartbeat,
                                     schedule_to_close, schedule_to_start,
                                     start_to_close, serialize_arguments)
        super(SWFActivityProxy, self).__init__(retry, error_handling,
                                               deserialize_result)

    @contextmanager
    def options(self, task_list=_sentinel, heartbeat=_sentinel,
                schedule_to_close=_sentinel, schedule_to_start=_sentinel,
                start_to_close=_sentinel, retry=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, heartbeat, schedule_to_close,
                                schedule_to_start, start_to_close):
            with super(SWFActivityProxy, self).options(retry, error_handling):
                yield

    def schedule(self, scheduler, *args, **kwargs):
        return self._spec.schedule(*args, **kwargs)


class SWFWorkflowProxy(TaskProxy):
    def __init__(self, name, version, task_list=None, decision_duration=None,
                 workflow_duration=None, retry=[0, 0, 0], error_handling=False,
                 serialize_arguments=serialize_arguments,
                 deserialize_result=deserialize_result):
        self._spec = SWFWorkflowSpec(name, version, task_list,
                                     decision_duration, workflow_duration,
                                     serialize_arguments)
        super(SWFWorkflowProxy, self).__init__(retry, error_handling,
                                               deserialize_result)

    @contextmanager
    def options(self, task_list=_sentinel, decision_duration=_sentinel,
                workflow_duration=_sentinel, retry=_sentinel,
                error_handling=_sentinel):
        with self._spec.options(task_list, decision_duration,
                                workflow_duration):
            with super(SWFWorkflowProxy, self).options(retry, error_handling):
                yield

    def schedule(self, swf_decisions, call_key, a, kw):
        call_key = '%s-%s' % (uuid.uuid4(), call_key)
        return self._spec.schedule(swf_decisions, call_key, a, kw)


def swf_activity(version, task_list=None, heartbeat=None,
                 schedule_to_close=None, schedule_to_start=None,
                 start_to_close=None, name=None):

    def wrapper(activity_factory):
        def callback(scanner, f_name, ob):
            if name is not None:
                f_name = name
            activity_spec = SWFActivitySpec(
                f_name, version, task_list, heartbeat, schedule_to_close,
                schedule_to_start, start_to_close)
            scanner.registry.add(activity_spec, activity_factory)
        venusian.attach(activity_factory, callback, category='activity')
        return activity_factory
    return wrapper


def swf_workflow(version, task_list=None, workflow_duration=None,
                 decision_duration=None, name=None):

    def wrapper(workflow_factory):

        scanner.attach(
            'swf_workflows',
            workflow_factory,
            version=version,
            task_list=task_list,
            workflow_duration=workflow_duration,
            decision_duration=decision_duration,
        )

        def callback(scanner, f_name, ob):
            if name is not None:
                f_name = name
            workflow_spec = SWFWorkflowSpec(
                f_name, version, task_list, decision_duration,
                workflow_duration)
            scanner.registry.add(workflow_spec, workflow_factory)
        venusian.attach(workflow_factory, callback, category='workflow')
        return workflow_factory
    return wrapper


class SWFTaskRegistry(TaskRegistry):
    def register_remote(self, swf_client):
        unregistered = []
        # sort it to be deterministic, helps on tests
        for spec in sorted(self._registry.keys()):
            if not spec.register_remote(swf_client):
                unregistered.append(spec)
        return unregistered


class SWFScanner(Scanner):
    def __init__(self, registry=None):
        if registry is None:
            registry = SWFTaskRegistry()
        super(SWFScanner, self).__init__(registry)

    def register_remote(self, swf_client):
        return self._registry.register_remote(swf_client)
