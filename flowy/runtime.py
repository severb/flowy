from collections import namedtuple
from contextlib import contextmanager
from functools import partial

from boto.swf.exceptions import SWFResponseError


class Heartbeat(object):
    def __init__(self, client, token):
        self._client = client
        self._token = token

    def __call__(self):
        try:
            self._client.record_activity_task_heartbeat(task_token=self._token)
        except SWFResponseError:
            return False
        return True


_OBase = namedtuple(
    typename='_Options',
    field_names=[
        'heartbeat',
        'schedule_to_close',
        'schedule_to_start',
        'start_to_close',
        'workflow_duration',
        'decision_duration',
        'child_policy',
        'task_list',
        'retry',
        'delay',
        'error_handling'
    ]
)


class _Options(_OBase):
    def update_with(self, other):
        t_pairs = zip(other, self)
        updated_fields = [x if x is not None else y for x, y in t_pairs]
        return _Options(*updated_fields)


class BoundProxyRuntime(object):
    def __init__(self, decision_runtime, decision_task):
        self._decision_runtime = decision_runtime
        self._decision_task = decision_task

    def __getattr__(self, proxy_name):
        proxy = getattr(self._decision_task, proxy_name)
        if not callable(proxy):
            raise AttributeError('%r is not callable' % proxy_name)
        return partial(proxy, self._decision_runtime)

    def options(self,
                heartbeat=None,
                schedule_to_close=None,
                schedule_to_start=None,
                start_to_close=None,
                workflow_duration=None,
                decision_duration=None,
                task_list=None,
                retry=None,
                delay=None,
                error_handling=None):
        self._decision_runtime.options(
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            workflow_duration=workflow_duration,
            decision_duration=decision_duration,
            task_list=task_list,
            retry=retry,
            delay=delay,
            error_handling=error_handling
        )


class ContextOptionsRuntime(object):
    def __init__(self, decision_runtime):
        self._decision_runtime = decision_runtime
        default_options = _Options(
            heartbeat=None,
            schedule_to_close=None,
            schedule_to_start=None,
            start_to_close=None,
            workflow_duration=None,
            decision_duration=None,
            task_list=None,
            retry=3,
            delay=0,
            error_handling=False
        )
        self._options_stack = [default_options]

    def remote_activity(self, result_deserializer,
                        heartbeat=None,
                        schedule_to_close=None,
                        schedule_to_start=None,
                        start_to_close=None,
                        task_list=None,
                        retry=None,
                        delay=None,
                        error_handling=None):
        options = _Options(
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            workflow_duration=None,
            decision_duration=None,
            task_list=task_list,
            retry=retry,
            delay=delay,
            error_handling=error_handling)
        new_options = options.update_with(self._options_stack[-1])
        self._decision_runtime.remote_activity(
            result_deserializer=result_deserializer,
            heartbeat=new_options.heartbeat,
            schedule_to_close=new_options.schedule_to_close,
            schedule_to_start=new_options.schedule_to_start,
            start_to_close=new_options.start_to_close,
            task_list=new_options.task_list,
            retry=new_options.retry,
            delay=new_options.delay,
            error_handling=new_options.error_handling
        )

    def remote_subworkflow(self, heartbeat, result_deserializer):
        pass

    @contextmanager
    def options(self,
                heartbeat=None,
                schedule_to_close=None,
                schedule_to_start=None,
                start_to_close=None,
                workflow_duration=None,
                decision_duration=None,
                child_policy=None,
                task_list=None,
                retry=None,
                delay=None,
                error_handling=None):
        options = _Options(
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            workflow_duration=workflow_duration,
            decision_duration=decision_duration,
            child_policy=child_policy,
            task_list=task_list,
            retry=retry,
            delay=delay,
            error_handling=error_handling
        )
        new_options = self._options_stack[-1].update_with(options)
        self._options_stack.append(new_options)
        yield
        self._options_stack.pop()


class DecisionRuntime(object):
    def __init__(self, client, token):
        self._client = client
        self._token = token

    def remote_activity(self, heartbeat, result_deserializer):
        pass

    def remote_subworkflow(self, heartbeat, result_deserializer):
        pass
