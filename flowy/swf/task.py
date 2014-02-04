from flowy.swf import SWFTaskId
from flowy.task import ActivityProxy as AP
from flowy.task import WorkflowProxy as WP


class ActivityProxy(AP):
    def __init__(self, name, version, **kwargs):
        super(ActivityProxy, self).__init__(
            task_id=SWFTaskId(name=name, version=version), **kwargs
        )


class WorkflowProxy(WP):
    def __init__(self, name, version, **kwargs):
        super(WorkflowProxy, self).__init__(
            task_id=SWFTaskId(name=name, version=version), **kwargs
        )
