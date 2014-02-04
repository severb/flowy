from flowy.scanner import activity as a
from flowy.scanner import workflow as w
from flowy.swf import SWFTaskId


def activity(name, version, task_list, **kwargs):
    return a(
        task_id=SWFTaskId(name=name, version=version),
        task_list=task_list,
        **kwargs
    )


def workflow(name, version, task_list, **kwargs):
    return w(
        task_id=SWFTaskId(name=name, version=version),
        task_list=task_list,
        **kwargs
    )
