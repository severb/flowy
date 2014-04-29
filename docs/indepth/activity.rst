.. _activity:

Activities
==========

An activity task is the smallest computational part of a workflow. You can
think of it as a function: it can receive an input, do some processing and it
can return something. The easiest way to implement an activity is by
subclassing ``Activity`` and overriding the ``run`` method. This provides some
convenience for you like automatic error handling, input deserialization and
result serialization.

A simple echo activity that returns the input it receives looks like this::

    class Echo(Activity):
        def run(self, value):
            return value


Passing Data In and Out
-----------------------

The entire communication between an activity and a workflow is made exclusively
through the arguments and the return value of the ``run`` method.  Just like
with any regular Python method, you can provide some default values for your
arguments, making them optional when the workflow schedules the activity to
run. You can also receive variable arguments, both positional and keyword.
Actually, you can use any valid Python method signature for your activity::

    class Echo(Activity):
        def run(self, value=None, *args, **kwargs):
            return (value,) + tuple(args) + tuple(kwargs.values())

There are some limitations on the values that can be passed in or out of the
activity: all of them must be JSON serializable. That's because the default
transport implementation uses JSON to pass data to and back from Amazon. You
can change this by overriding the ``_serialize_result`` and
``_deserialize_arguments`` methods, but this is rarely enough as the workflow
must also be aware of the changes. For a complete explanation on how to change
the transport protocol see :ref:`transport`.


Dealing With Exceptions
-----------------------

The activities have automatic exception handling provided by the ``Activity``
superclass. This means that any unhandled exception raised inside the ``run``
method is transformed into an activity failed type of result. There are certain
mechanisms in the workflow to detect and deal with this when it happens. Of
course, you can still catch and handle exceptions in the activity itself, but
if you want to signal an error, it's safe to raise exceptions from the
activity::

    class Echo(Activity):
        def run(self, value):
            if value == 1:
                raise ValueError("Can't echo 1.")
            return value


Reporting Progress
------------------

An activity can send back progress notifications from time to time using the
``hearbeat`` method so that the workflow knows that it's still alive. The
return value of this call can then be used to determine if the activity should
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
times out, either because it failed to send updates as often as the workflow
expected or because the entire execution duration exceeded the maximum accepted
by the workflow, the result of the call to ``heartbeat`` will be negative. In
either case it's safe to finish the execution after an optional cleanup. We
could also continue running the activity, but the final result produced by this
activity would be disregarded anyway because of the time out. More then that,
continuing to run an activity that has already timed out is a waste of worker
resources.

.. warning::

    Make sure you don't send heartbeats in tight loops. Sending a lot of them
    in a short period of time may trigger backend rate limiting errors.


Activity Registration and Discovery
-----------------------------------

In order to discover the activities you implement Flowy uses `venusian`_ to
register and later scan for existing implementations. For an activity to be
discoverable it must be decorated with the ``activity`` decorator like so::

    from flowy.swf.scanner import activity

    @activity(name='echo', version=1, task_list='my_tasklist')
    class Echo(Activity):
        def run(self, value):
            return value

This not only provides some required metadata for the activity, but also
makes it so that later, when the activity worker is started, it can scan for
existing activities.

.. function:: flowy.swf.boilerplate.start_activity_worker(domain, task_list, layer1=None, reg_remote=True, loop=-1, package=None, ignore=None)

    Start the main loop that pulls activities from the *task_list* belonging to
    the *domain* and runs them.

    It begins by scanning for activity implementations in the *package* list of
    Python package or module objects skipping the ones in the *ignore* list.
    These two arguments have the same semantic value as the ones in the venusian
    `scan`_ method. By default, if no modules are provided it scans the current
    module.

    If you want to construct and customize your own SWF `Layer1`_ instance you
    can pass it in through the *layer1* attribute.

    If *reg_remote* flag is set it attempts to register the activities
    remotely. The activities need be registered remotely before a workflow can
    schedule any of them. This flag makes it possible to start a lot of workers
    at the same time without all of them doing the remote registration calls.

    The *loop* is mainly used for testing to force the main loop to run only
    for a limited number of iterations. By default the main loop runs forever.


Default Configuration
---------------------

The ``activity`` decorator does more than just making the implementation
discoverable, it's also  used to provide activity metadata. The name and the
version are required and are used to identify the activity. The task list is
also required but, like the other timeout related values, it's only a default
value - it can be overridden from the workflow.

.. function:: flowy.swf.scanner.activity(name, version, task_list, heartbeat=None, schedule_to_close=420, schedule_to_start=120, start_to_close=300)

    This function returns a decorator that can be used to register activity
    implementations.

    The *name* and the *version* are used to identify the activity being
    decorated. The workflow will need to know these values in order to schedule
    the activity. By default it will schedule this type of activities to the
    specified *task_list*.

    The other values are used to control different types of timeout limits.
    All of them serve just as default values and can be overridden by a
    workflow:

        * *heartbeat* - the maximum number of seconds between two consecutive
          heartbeat notifications; by default no limit is set.
        * *schedule_to_close* - the number of seconds since the activity was
          scheduled until it can finish. This value must usually be larger than
          *schedule_to_start* and *start_to_close*.
        * *schedule_to_start* - the duration in seconds this activity can spend
          queued.
        * *start_to_close* - how many seconds the activity can run for before
          it will timeout.

.. seealso::

    `Amazon SWF Timeout Types`_
        A document describing in great detail the different types of timeout
        timers.


Updating the Activity Implementation
------------------------------------

TBD


Async Activities
----------------

An activity need not return a value right away. Instead you can raise a
``SuspendTask`` exception to finish the execution without returning a value and
free the worker. Later, maybe on a different system, you can use
``async_scheduler`` to finish the execution. This is useful when an activity is
asynchronous - for example it waits for a human approval in order to continue::

    from flowy.exception import SuspendTask

    class Echo(Activity):
        def run(self, value):
            self.persist_in_3rd_party_system(value, self.token)
            raise SuspendTask

Here we persist the value we received together with a token. The token is used
to identify this activity when we decide to finish it and it's always present
in an activity as the ``.token`` property.

.. function:: flowy.swf.boilerplate.async_scheduler(domain, layer1=None)

    A factory for instances that can control asynchronous activities. The
    *domain* must be the same with the domain of the activities you want to
    control.

    If you want to construct and customize your own SWF `Layer1`_ instance you
    can pass it in through the *layer1* attribute.

    Objects returned by this factory implement the following methods:

    .. method:: heartbeat(token)

        Send a heartbeat for the activity identified by *token*. The same as
        calling the ``heartbeat()`` method on the activity itself.

    .. method:: complete(token, result) 

        Complete the activity identified by *token* with the *result* value.
        This is similar with returning a value directly from the activity
        itself.

    .. method:: fail(token, reason)

        Complete the activity identified by *token* with an error. Similar as
        raising an exception inside the activity with the *reason* message.


.. _venusian: http://docs.pylonsproject.org/projects/venusian/
.. _scan: http://docs.pylonsproject.org/projects/venusian/en/latest/api.html#venusian.Scanner.scan
.. _Layer1: http://boto.readthedocs.org/en/latest/ref/swf.html#boto.swf.layer1.Layer1
.. _Amazon SWF Timeout Types: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-timeout-types.html
