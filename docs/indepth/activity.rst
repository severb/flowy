.. _activity:

Activities
==========

An activity task is the smallest computational part of a workflow. You can
think of it as a function: it can get an input, do some processing and may
return something back. The easiest way to implement an activity is by
subclassing the ``Activity`` class and overriding the ``run`` method. This
provides some convenience for you like automatic error handling, input
deserialization and result serialization.

A simple echo activity that returns the input it receives looks like this::

    class Echo(Activity):
        def run(self, value):
            return value


Passing Data In and Out
-----------------------

The entire communication between an activity and a workflow is made exclusively
trough the arguments and the return value of the ``run`` method.  Just as any
regular Python method you can provide some default values for your arguments
making them optional later when the workflow schedules the activity to run. You
can also receive variable arguments both positional or keyword. Actually, you
can use any valid Python method signature for your activity::

    class Echo(Activity):
        def run(self, value=None, *args, **kwargs):
            return (value,) + tuple(args) + tuple(kwargs.values())

There are some limitations on the values that can be passed in or out of the
activity: all of them must be JSON serializable. That's because the default
transport implementation uses JSON to pass data to and back from Amazon. You
can change this by overriding the ``_serialize_result`` and
``_deserialize_arguments`` methods but this is rarely enough as the workflow
must also be aware of the changes. For a complete explanation on how to change
the transport protocol see :ref:`transport`.


Dealing With Exceptions
-----------------------

The activities have automatic exception handling provided by the ``Activity``
subclass. This means that any unhandled exception inside the ``run`` method is
transformed in an activity failed type of result. There are certain mechanisms
in the workflow to detect and deal with this when it happens. Of course, you
can still catch and handle exceptions in the activity itself, but if you want
to signal an error it's safe to raise exceptions from the activity::

    class Echo(Activity):
        def run(self, value):
            if value == 1:
                raise ValueError("Can't echo 1.")
            return value


Reporting Progress
------------------

An activity can send back progress notifications from time to time using the
``hearbeat`` method so that the workflow knows that it's still alive. The
return value of this calls can then be used to determine if the activity should
continue running or abandon the progress. Let's see how this looks in
practice::

    import time

    class Echo(Activity):
        def run(self, value):
            for x in range(10):
                time.sleep(5)
                if not self.hearbeat():
                    return
            return value

As you can see here we send a heartbeat every five seconds. If the activity
times out either because it failed to send updates as often as the workflow
expected or because the entire execution duration exceeded the maximum accepted
by the workflow the ``heartbeat`` call result will be negative. In either case
it's safe to finish the execution after an optional cleanup. We could also
continue running the activity but the final result produced by this activity
would be disregarded anyway because of the timed out. More then that, running
an activity that already timed out is a wast of the worker processing time.

.. warning::

    Make sure you don't send heartbeats in tight loops. Sending a lot of them
    in a short period of time may trigger backend rate limits errors.


Implementation Registration and Discovery
-----------------------------------------

In order to discover the activities you implement Flowy uses `venusian`_ to
register and later scan for existing implementations. For an activity to be
discoverable it must be decorated with the ``activity`` decorator like so::

    from flowy.swf.scanner import activity

    @activity(name='echo', version=1, task_list='my_tasklist')
    class Echo(Activity):
        def run(self, value):
            return value

This doesn't only provides some required metadata for the activity but also
makes it so that later, when the activity worker is started, it can scan for
existing activities.

.. function:: flowy.swf.boilerplate.start_activity_worker(domain, task_list, layer1=None, reg_remote=True, loop=-1, package=None, ignore=None)

    Start the main loop that pulls activities from the *task_list* belonging to
    the *domain* and runs them.

    It begins by scanning for activity implementations in the *package* list of
    Python package or module objects skipping the ones in the *ignore* list.
    This two arguments have the same semantic as the ones in the venusian
    `scan`_ method. By default, if no modules are provided it scans the current
    module.

    If you want to construct and customize your own SWF `Layer1`_ instance you
    can pass it in trough the *layer1* attribute.

    If *reg_remote* flag is set it attempts to register the activities
    remotely. The activities need be registered remotely before a workflow can
    schedule any of them. This flag makes it possible to start a lot of workers
    at the same time without all of them doing the remote registration calls.

    The *loop* is mainly used for testing to force the main loop only run for a
    limited number of iterations. By default the main loop runs forever.


Default Configuration
---------------------

TBD


Async Activities
----------------

TBD


.. _venusian: http://docs.pylonsproject.org/projects/venusian/
.. _scan: http://docs.pylonsproject.org/projects/venusian/en/latest/api.html#venusian.Scanner.scan
.. _Layer1: http://boto.readthedocs.org/en/latest/ref/swf.html#boto.swf.layer1.Layer1
