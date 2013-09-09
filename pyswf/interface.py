from zope.interface import Interface, Attribute


class IWorkflowClient(Interface):
    def register(name, version, domain, task_list, workflow_runner,
        execution_start_to_close=3600,
        task_start_to_close=60,
        child_policy='TERMINATE'
    ):
        """Register a workflow implementation."""

    def run():
        """Kick-start the workflow polling and running cycle."""


class IWorkflowResponse(Interface):
    name = Attribute("The name of the workflow.")
    version = Attribute("The version of the workflow.")
    context = Attribute("The context of the previous decision.")

    def schedule(activity_name, activity_version, input):
        """Schedule a new activity to be run as part of this decision."""

    def suspend(context):
        """Suspend the workflow process and register all activities
        scheduled for running.

        """

    def complete(result):
        """Finishes the workflow process with the specified result."""

    def __iter__():
        """Iterate trough all new IWorkflowEvent instances since the last
        decision.

        """


class IWorkflowEvent(Interface):
    def update(context):
        """Update the context state with this specific event."""


class IWorkflowContext(Interface):
    def is_activity_scheduled(call_id):
        """Determine whether any activity was already scheduled for this
        call_id.

        """

    def activity_result(call_id, default=None):
        """Try to get the result for the activity scheduled with this call_id.
        If no result is available yet the default will be returned instead.

        """

    def is_activity_result_error(call_id):
        """Determine if the result of the activity scheduled with this call_id
        represents an error or a valid result.

        """

    def is_activity_timedout(call_id):
        """Determine if the activity scheduled with this call_id has not
        finished running in time.

        """

    def set_scheduled(call_id):
        pass

    def set_result(call_id, result):
        pass

    def set_error(call_id, error):
        pass

    def set_timed_out(call_id):
        pass

    def finished(call_id, result):
        pass

    def serialize():
        """Returns a string representation for this context suited for
        persistance between decisions.

        """


class IWorkflowRunner(Interface):
    def resume(workflow_response, workflow_context):
        """Resume the workflow process."""
