from flowy.scanner import activity as a
from flowy.scanner import workflow as w
from flowy.swf import SWFTaskId


def activity(name, version, task_list,
             heartbeat=None,
             schedule_to_close=None,
             schedule_to_start=None,
             start_to_close=None):
    return a(
        task_id=SWFTaskId(name=str(name), version=str(version)),
        task_list=task_list,
        heartbeat=heartbeat,
        schedule_to_close=schedule_to_close,
        schedule_to_start=schedule_to_start,
        start_to_close=start_to_close
    )


def workflow(name, version, task_list,
             decision_duration=None,
             workflow_duration=None):
    return w(
        task_id=SWFTaskId(name=str(name), version=str(version)),
        task_list=task_list,
        decision_duration=decision_duration,
        workflow_duration=workflow_duration,
    )
