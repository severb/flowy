from flowy.scanner import activity_task as a_t
from flowy.swf import SWFTaskId


def activity_task(name, version, task_list,
                  heartbeat=None,
                  schedule_to_close=None,
                  schedule_to_start=None,
                  start_to_close=None):
    return a_t(
        task_id=SWFTaskId(name=str(name), version=str(version)),
        task_list=task_list,
        heartbeat=heartbeat,
        schedule_to_close=schedule_to_close,
        schedule_to_start=schedule_to_start,
        start_to_close=start_to_close
    )
