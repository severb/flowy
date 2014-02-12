class SuspendTask(Exception):
    """ Raised to suspend the task run.

    This happens when a worklfow needs to wait for an activity or in case of an
    async activity.
    """


class TaskError(Exception):
    """ Raised from an activity or subworkflow task if error handling is
    enabled and the task fails.
    """


class TaskTimedout(TaskError):
    """ Raised from an activity or subworkflow task if any of its timeout
    timers were exceeded.
    """
