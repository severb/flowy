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

Similarly with an activity, you can pass data in a workflow using any
combination of arguments on the ``run`` method. After the workflow finished its
processing, you can pass data out simply by returning from the ``run`` method.
The only thing to keep in mind is that the data that gets in and out must be
(by default) JSON serializable. That’s because the default transport
implementation uses JSON to pass data to and back from the backend. You can
change this by overriding the ``_serialize_result``, ``_deserialize_arguments``
and ``_serialize_restart_arguments`` methods, but this is rarely enough as the
workflow starter implementation  must also be aware of the changes. For a
complete explanation on how to change the transport protocol see
:ref:`transport`.


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

As you can see the proxies can also override some of the defaults used when the
activity or the workflow was registered.


Task Result
-----------


Execution Model
---------------
