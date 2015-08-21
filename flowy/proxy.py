import json

from flowy.operations import first
from flowy.result import copy_result_proxy
from flowy.result import error
from flowy.result import placeholder
from flowy.result import result
from flowy.result import SuspendTask
from flowy.result import timeout
from flowy.result import wait
from flowy.serialization import dumps
from flowy.serialization import loads
from flowy.serialization import traverse_data
from flowy.utils import logger


__all__ = ['Proxy']


class Proxy(object):
    """A proxy bound to a task_exec_history and a decision object.

    This is what gets passed as a dependency in a workflow and has most of the
    scheduling logic that can be reused across different backends.
    The real scheduling is dispatched to the decision object.
    """

    def __init__(self, task_exec_history, task_decision, retry=(0, ),
                 serialize_input=None, deserialize_result=None):
        """Init the proxy object.

        The task execution history contains the execution history and is
        used to decide what new tasks should be scheduled.
        The scheduling of new tasks or execution or the execution failure is
        delegated to the task decision object.
        """
        self.task_exec_history = task_exec_history
        self.task_decision = task_decision
        self.retry = retry
        self.call_number = 0
        if serialize_input is not None:
            self.serialize_input = serialize_input
        if deserialize_result is not None:
            self.deserialize_result = deserialize_result

    def __call__(self, *args, **kwargs):
        """Consult the execution history for results or schedule a new task.

        This is method gets called from the user workflow code.
        When calling it, the task it refers to can be in one of the following
        states: RUNNING, READY, FAILED, TIMEDOUT or NOTSCHEDULED.

        * If the task is RUNNING this returns a Placeholder. The Placeholder
          interrupts the workflow execution if its result is accessed by
          raising a SuspendTask exception.
        * If the task is READY this returns a Result object. Calling the result
          method on this object will just return the final value the task
          produced.
        * If the task is FAILED this returns an Error object. Calling the
          result method on this object will raise a TaskError exception
          containing the error message set by the task.
        * In case of a TIMEOUT this returns an Timeout object. Calling the
          result method on this object will raise TaskTimedout exception, a
          subclass of TaskError.
        * If the task was NOTSCHEDULED yet:
            * If any errors in arguments, propagate the error by returning
              another error.
            * If any placeholders in arguments, don't do anything because there
              are unresolved dependencies.
            * Finally, if all the arguments look OK, schedule it for execution.
        """
        task_exec_history = self.task_exec_history
        call_number = self.call_number
        self.call_number += 1
        r = placeholder()
        for retry_number, delay in enumerate(self.retry):
            if task_exec_history.is_timeout(call_number, retry_number):
                continue
            if task_exec_history.is_running(call_number, retry_number):
                break  # result = Placehloder
            if task_exec_history.has_result(call_number, retry_number):
                value = task_exec_history.result(call_number, retry_number)
                order = task_exec_history.order(call_number, retry_number)
                try:
                    value = self.deserialize_result(value)
                except Exception as e:
                    logger.exception('Error while deserializing the activity result:')
                    self.task_decision.fail(e)
                    break  # result = Placeholder
                r = result(value, order)
                break
            if task_exec_history.is_error(call_number, retry_number):
                err = task_exec_history.error(call_number, retry_number)
                order = task_exec_history.order(call_number, retry_number)
                r = error(err, order)
                break
            traversed_args, (err, placeholders) = traverse_data([args, kwargs])
            if err:
                r = copy_result_proxy(err)
                break
            if placeholders:
                break  # result = Placeholder
            t_args, t_kwargs = traversed_args
            try:
                input_data = self.serialize_input(*t_args, **t_kwargs)
            except Exception as e:
                logger.exception('Error while serializing the task input:')
                self.task_decision.fail(e)
                break  # result = Placeholder
            self.task_decision.schedule(call_number, retry_number, delay, input_data)
            break  # result = Placeholder
        else:
            # No retries left, it must be a timeout
            order = task_exec_history.order(call_number, retry_number)
            r = timeout(order)
        return r

    @staticmethod
    def serialize_input(*args, **kwargs):
        return dumps([args, kwargs])

    @staticmethod
    def deserialize_result(result):
        return loads(result)
