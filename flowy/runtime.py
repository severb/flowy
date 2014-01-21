from contextlib import contextmanager
from functools import partial

from flowy import int_or_none, str_or_none


_sentinel = object()


class ProxyRuntime(object):
    def __init__(self, decision_runtime, decision_task):
        self._decision_runtime = decision_runtime
        self._decision_task = decision_task

    def __getattr__(self, proxy_name):
        proxy = getattr(self._decision_task, proxy_name)
        if not callable(proxy):
            raise AttributeError('%r is not callable' % proxy_name)
        return partial(proxy, self._decision_runtime)

    def options(self, **kwargs):
        self._decision_runtime.options(**kwargs)


class OptionsRuntime(object):
    def __init__(self, decision_runtime):
        self._decision_runtime = decision_runtime
        self._activity_options_stack = [dict()]
        self._subworkflow_options_stack = [dict()]

    def remote_activity(self, input, result_deserializer, heartbeat,
                        schedule_to_close, schedule_to_start, start_to_close,
                        task_list, retry, delay, error_handling):
        options = dict(
            heartbeat=int_or_none(heartbeat),
            schedule_to_close=int_or_none(schedule_to_close),
            schedule_to_start=int_or_none(schedule_to_start),
            start_to_close=int_or_none(start_to_close),
            task_list=str_or_none(task_list),
            retry=int(retry),
            delay=int(delay),
            error_handling=bool(error_handling)
        )
        options.update_with(self._activity_options_stack[-1])
        self._decision_runtime.remote_activity(
            input=str(input),
            result_deserializer=result_deserializer,
            **options
        )

    def remote_subworkflow(self, input, result_deserializer, workflow_duration,
                           decision_duration, task_list, retry, delay,
                           error_handling):
        options = dict(
            workflow_duration=int_or_none(workflow_duration),
            decision_duration=int_or_none(decision_duration),
            task_list=str_or_none(task_list),
            retry=int(retry),
            delay=int(delay),
            error_handling=bool(error_handling)
        )
        options.update_with(self._subworkflow_options_stack[-1])
        self._decision_runtime.remote_subworkflow(
            input=str(input),
            result_deserializer=result_deserializer,
            **options
        )

    @contextmanager
    def options(self,
                heartbeat=_sentinel,
                schedule_to_close=_sentinel,
                schedule_to_start=_sentinel,
                start_to_close=_sentinel,
                workflow_duration=_sentinel,
                decision_duration=_sentinel,
                task_list=_sentinel,
                retry=_sentinel,
                delay=_sentinel,
                error_handling=_sentinel):
        a_options = dict()
        s_options = dict()
        if heartbeat is not _sentinel:
            a_options['heartbeat'] = int_or_none(heartbeat)
        if schedule_to_close is not _sentinel:
            a_options['schedule_to_close'] = int_or_none(schedule_to_close)
        if schedule_to_start is not _sentinel:
            a_options['schedule_to_start'] = int_or_none(schedule_to_start)
        if start_to_close is not _sentinel:
            a_options['start_to_close'] = int_or_none(start_to_close)
        if workflow_duration is not _sentinel:
            s_options['workflow_duration'] = int_or_none(workflow_duration)
        if decision_duration is not _sentinel:
            s_options['decision_duration'] = int_or_none(decision_duration)
        if task_list is not _sentinel:
            a_options['task_list'] = str_or_none(task_list)
            s_options['task_list'] = str_or_none(task_list)
        if retry is not _sentinel:
            a_options['retry'] = int(retry)
            s_options['retry'] = int(retry)
        if delay is not _sentinel:
            a_options['delay'] = int(delay)
            s_options['delay'] = int(delay)
        if error_handling is not _sentinel:
            a_options['error_handling'] = bool(error_handling)
            s_options['error_handling'] = bool(error_handling)
        self._activity_options_stack.append(
            dict(self._activity_options_stack[-1])
            .update(a_options)
        )
        self._subworkflow_options_stack.append(
            dict(self._subworkflow_options_stack[-1])
            .update(a_options)
        )
        yield
        self._activity_options_stack.pop()
        self._subworkflow_options_stack.pop()
