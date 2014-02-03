from flowy.swf import SWFTaskId
from flowy.task import ActivityProxy as AP
from flowy.task import WorkflowProxy as WP


class ActivityProxy(AP):
    def __init__(self, name, version,
                 heartbeat=None,
                 schedule_to_close=None,
                 schedule_to_start=None,
                 start_to_close=None,
                 task_list=None,
                 retry=3,
                 delay=0):
        super(ActivityProxy, self).__init__(
            task_id=SWFTaskId(name=name, version=version),
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            task_list=task_list,
            retry=retry,
            delay=delay
        )


class WorkflowProxy(WP):
    def __init__(self, name, version,
                 decision_duration=None,
                 workflow_duration=None,
                 task_list=None,
                 retry=3,
                 delay=0):
        super(WorkflowProxy, self).__init__(
            task_id=SWFTaskId(name=name, version=version),
            decision_duration=decision_duration,
            workflow_duration=workflow_duration,
            task_list=task_list,
            retry=retry,
            delay=delay
        )
