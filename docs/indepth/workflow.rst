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

In order to discover the workflows you implement Flowy uses `venusian`_ to
register and later scan for existing implementations. For a workflow to be
discoverable it must be decorated with the ``workflow`` decorator like so::

    from flowy.swf.scanner import workflow

    @workflow(name='echo', version=1, task_list='my_tasklist')
    class Echo(Workflow):
        def run(self, value):
            return value

This not only provides some required metadata for the workflow, but also
makes it so that later, when the workflow worker is started, it can scan for
existing activities.

.. function:: flowy.swf.boilerplate.start_workflow_worker(domain, task_list, layer1=None, reg_remote=True, loop=-1, package=None, ignore=None)

    Start the main loop that pulls workflows that can make progress from the
    *task_list* belonging to the *domain* and runs them. This is a single
    threaded/single process loop. If you want to distribute the workload it's
    up to you to start multiple loops.

    It begins by scanning for workflow implementations in the *package* list of
    Python package or module objects skipping the ones in the *ignore* list.
    These two arguments have the same semantic value as the ones in the
    venusian `scan`_ method. By default, if no modules are provided it scans
    the current module.

    If you want to construct and customize your own SWF `Layer1`_ instance you
    can pass it in through the *layer1* attribute.

    If *reg_remote* flag is set it attempts to register the workflows remotely.
    A workflow needs to be registered remotely before it can be started. This
    flag makes it possible to start a lot of workers at the same time without
    all of them doing the remote registration calls.

    The *loop* is mainly used for testing to force the main loop to run only
    for a limited number of iterations. By default the main loop runs forever.


Default Configuration
---------------------

The ``workflow`` decorator does more than just making the implementation
discoverable, it’s also used to provide workflow metadata. The `name` and the
`version` are required and are used to identify the workflow. The `task_list`
is also required but, like the other timeout related values, it’s only a
default value - it can be overridden by the sub-workflow proxies or when
starting a new workflow.

.. function:: flowy.swf.scanner.workflow(name, version, task_list, workflow_duration=3600, decision_duration=60)

    This function returns a decorator that can be used to register workflow
    implementations.

    The *name* and the *version* are used to identify the workflow being
    decorated. The workflow starter will need to know these values in order to
    schedule the workflow. By default it will schedule this type of workflows
    to the specified *task_list*.

    The other values are used to control different types of timeout limits.
    All of them serve just as default values and can be overridden by the
    sub-workflow proxies or when starting a new workflow:

        * *workflow_duration* - the maximum number of seconds this entire
          workflow execution can run for.
        * *decision_duration* - the maximum number of seconds a single decision
          execution can run for. See :ref:`execution_model` for more details.

.. seealso::

    `Amazon SWF Timeout Types`_
        A document describing in great detail the different types of timeout
        timers.


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

.. class:: flowy.swf.task.ActivityProxy(name, version, heartbeat=None, schedule_to_close=None, schedule_to_start=None, start_to_close=None, task_list=None, retry=3, delay=0, error_handling=False)

    Create a proxy for the activity identified by `name` and `version`. The
    recommended way to use the proxy is as a property on the ``Workflow``
    subclasses.

        * *heartbeat*, *schedule_to_close*, *schedule_to_start*,
          *start_to_close* and *task_list* - can be used to override the
          default values used when the activity was registered.
        * *retry* - the number of automatic retries of the activity in case of
          timeout errors.
        * *delay* - add a delay before the activity is scheduled.
        * *error_handling* - use it in case you want to manually handle
          exceptions raised inside the activity. See :ref:`error` for more
          details.

.. class:: flowy.swf.task.WorkflowProxy(name, version, decision_duration=None, workflow_duration=None, task_list=None, retry=3, delay=0, error_handling=False)

    Create a proxy for the activity identified by `name` and `version`. The
    recommended way to use the proxy is as a property on the ``Workflow``
    subclasses.

        * *decision_duration*, *workflow_duration* and *task_list* - can be
          used to override the default values used when the workflow was
          registered.
        * *retry* - the number of automatic retries of the workflow in case of
          timeout errors.
        * *delay* - add a delay before the workflow is scheduled.
        * *error_handling* - use it in case you want to manually handle
          exceptions raised inside the workflow. See :ref:`error` for more
          details.


Task Result
-----------


.. _execution_model:

Execution Model
---------------


.. _venusian: http://docs.pylonsproject.org/projects/venusian/
.. _scan: http://docs.pylonsproject.org/projects/venusian/en/latest/api.html#venusian.Scanner.scan
.. _Layer1: http://boto.readthedocs.org/en/latest/ref/swf.html#boto.swf.layer1.Layer1
.. _Amazon SWF Timeout Types: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-timeout-types.html
