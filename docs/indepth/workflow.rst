.. _workflow:

Workflows
=========

A workflow coordinate activities and other sub-workflows to compose complex
behavior. You can think of it as a function that only contains the logic need
to know where and what to delegate to other functions. You can create a
workflow by subclassing the ``Workflow`` class and overriding the ``run``
method. Doing so gives you some some built-in convenience like granular
settings, error handling, automatic input deserialization and result
serialization.

A simple workflow that doesn't make use of any other activities or
sub-workflows looks very similar to an activity::

    class Echo(Workflow):
        def run(self, value):
            return value


Passing Data In and Out
-----------------------


Workflow Registration and Discovery
-----------------------------------


Default Configuration
---------------------


Task Proxies
------------

The power of a workflow is the orchestration of previously defined activities
and workflows. You can create references (or proxies) to your activities and
workflows by setting instances of ``ActivityProxy`` and ``WorkflowProxy`` as
properties on your workflow class. Then, you can use this properties as methods
to schedule a specific task to run. This is how it looks in practice::

    class MyWorkflow(Workflow):
        echo_activity_proxy = ActivityProxy(name='Echo', version='v1', heartbeat=10)
        echo_workflow_proxy = WorkflowProxy(name='Echo', version=2, workflow_duration=30)

        def run(self):
            self.echo_activity_proxy('activity echo')
            self.echo_workflow_proxy('workflow echo')

As you can see the the proxies can also override some of the defaults used when
the activity or the workflow was registered.


Execution Model
---------------


Result Synchronization
----------------------
