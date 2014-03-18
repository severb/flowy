.. _tutorial:

Tutorial
========

In this tutorial we'll create a workflow that resizes and classifies images
based on their predominant color. A typical workflow uses one or more
activities to do the actual data processing. Here we'll use three activities:
one for image resizing, another one for predominant color computation and the
last one will move images from one place to another. The workflow's
responsibility is to coordinate these activities and pass the data between
them. Each time an activity finishes its execution the workflow will decide
what needs to happen next. This decision can be one of the following: start or
retry activities, complete or abort the workflow execution.

Let's start by setting up the development environment and create a new domain.


Setting Up the Environment
--------------------------

To isolate the requirements for this tutorial let's create a new virtual
environment and install all the dependencies::

    $ virtualenv /tmp/flowytutorial/
    $ source /tmp/flowytutorial/bin/activate
    (flowytutorial)$ pip install flowy requests pillow # a fork of PIL

.. When should you see this?

After installation make sure `pillow`_ has JPEG support. You should see::

    --- JPEG support available


Registering a New Domain
------------------------

Before implementing our first workflow we need to define a domain in the Amazon
SWF service that will host the workflow and all its activities. The domain is
like a namespace, where the workflows and activities registered in the same
domain can see only each other. You can register a domain using the management
console and by following `these steps`_ - just make sure you name it
*flowytutorial*. You can also register a domain using the `boto`_ library:
launch your Python interpreter with the following two environment variables
set::

        (flowytutorial)$ AWS_ACCESS_KEY_ID=<your key> AWS_SECRET_ACCESS_KEY=<your secret> python

And then use the `register_domain`_ method like so:

        >>> from boto.swf.layer1 import Layer1
        >>> Layer1().register_domain('flowytutorial', 7)  # keep the run history for 1 week


The Image Resizing Activity
---------------------------

The workflows are using activities to delegate the actual processing. As we'll
see in the example below, you can implement an activity by overriding the
``run`` method of the ``Activity`` class. You can pass data in the activity
from the workflow and out from the activity to the workflow. Once we have a few
activities implemented we can spawn as many activity worker processes as we
need. Each worker will scan for available activity implementations and start
listening for incoming tasks. As soon as a workflow schedules a new activity,
one of the available workers will execute the activity code and return the
result.

Let's create an activity that will download and resize an image. Start by
creating a new file named ``activities.py`` and add the following content:

.. literalinclude:: activities.py
    :lines: 1-35
    :language: python

There is a lot going on in this activity so we'll go over it line by line.
Skipping all the imports we get to the first interesting part, which is the
``activity`` decorator:

.. literalinclude:: activities.py
    :lines: 12-13
    :language: python

The ``activity`` decorator has two different responsibilities. It registers
this activity implementation, so that later, when we spawn an activity worker,
the activity is discovered. At the same time, it provides some of the
properties for an activity, like its name, version and default task list. A
workflow can use the name and version defined here to schedule an execution of
this activity - we'll see exactly how later. The task lists control how
activities are routed to the worker processes. Whenever we spawn a worker
process we specify a task list to pull activities from, multiple workers being
allowed to pull from the same list. It is important that a worker process knows
how to run all the different activity types that will be scheduled on the list
it pulls from. The task list defined with the ``activity`` decorator only
serves as a default value - the workflow can override it for an activity,
scheduling it on a specific list. There are other things that can be set with
this decorator, like timeout values, error handling strategy and the number of
retries. For now we should be fine using the default values.

.. literalinclude:: activities.py
    :lines: 13-19
    :language: python

Moving forward we get to the class definition of the activity. Every activity
must be a subclass of ``Activity``. This provides some default preprocessing of
the input and output, exception handling and logging. The image resizing code
is inside the ``run`` method which is the activity entry point. It gets an URL
and a target size and starts by downloading and processing the image. In the
end it creates a temporary file where it stores the resulting new image and
returns the path to it. There are some restrictions on what can go in and out
an activity but we don't need to worry about that for now.

.. note::

    This tutorial tries to keep things easy to understand and not necessarily
    robust. As you may have noticed, we are saving the resized image to the
    local disk. This assumes that all your workers will run on the same
    machine. As soon as you start to distribute the work across multiple
    machines, you will have a problem. In that case, a better solution would be
    to use a shared storage system.


Processing the Image and Sending Updates
----------------------------------------

There are two activities that we still have to implement: one that computes the
most predominant color and another one that moves images on disk. Open
``activities.py`` and append the following code:

.. literalinclude:: activities.py
    :lines: 38-61
    :language: python

We changed a few things in this example so lets go over it. We'll start again
with the ``activity`` decorator:

.. literalinclude:: activities.py
    :lines: 38-39
    :language: python

Here we used positional arguments for the name, version (which is a string this
time) and task list. This gave us some space to squeeze in a `heartbeat` value.
The heartbeat represents the maximum duration in seconds in which an activity
must report back some progress. The value we set here, like with the task list,
is only a default value. A workflow using this activity has very granular
options for overriding it.

.. literalinclude:: activities.py
    :lines: 40-44
    :language: python

Jumping over the class definition we get to the ``run`` method. Here we can see
how the ``heartbeat`` method is used to report progress. The method returns a
boolean value indicating whether we were on time or not. In this case being on
time means that the time elapsed between the last heartbeat or since the
activity started and the current heartbeat is less then 15 seconds. If we are
on time we can continue with our processing; else we should abandon the
activity since it timed out and the result or further heartbeats will be
ignored. Any cleanup needed to stop the progress cleanly can be done at this
point. You can also see the heartbeat functionality used in the ``sum_colors``
method to report progress after each megapixel is processed.


Moving Files on Disk
--------------------

This will be a very simple activity that will rename a file. Append in
``activities.py`` the following code:

.. literalinclude:: activities.py
    :lines: 63-67
    :language: python

Hopefully, at this point, this should look familiar. The only new thing here is
the use of ``start_to_close``. This sets a limit of 10 seconds on how long
the activity can run before it will time out and be abandoned.


Running the Activity Worker
---------------------------

We finished writing all the activities that we need and it's time to start an
activity worker process. This process, once started, will continuously pull for
jobs from a task list and call the ``run`` method of one of the three
activities we have defined. The task list we'll pull from is called
*image_processing* and it's already defined as the default task list for each
activity. Another thing we need is the domain we created at the beginning of
the tutorial - it was named *flowytutorial*. So, once again open
``activities.py`` and append:

.. literalinclude:: activities.py
    :lines: 69-72
    :language: python

To start a worker run Python, while passing the Boto authentication environment
variables like so::

    (flowytutorial)$ AWS_ACCESS_KEY_ID=<your key> AWS_SECRET_ACCESS_KEY=<your secret> python activities.py


You don't have to limit yourself to only one process - start as many as you
want.  Because all of them will pull for jobs from the same task list, the
workload will be evenly spread. Actually, you won't see anything happening as
you run your workers because there is no workflow to schedule any activities.
Let's change that and write our first workflow!

.. seealso::

   :ref:`activities_py`
         The final version of ``activities.py`` file.
   :ref:`activity`
         In depth documentation on writing Activity Tasks.


Putting It All Together
-----------------------

A workflow is just like an activity - the only difference is that while an
activity does the actual processing or computation, the workflow doesn't do
much except coordinating the activities.

We'll need a new file for the workflow. Open ``workflow.py`` and add the
following code:

.. literalinclude:: workflow.py
    :language: python

The structure of this file is simlar with the one we have for activities, but
there are many subtle (and very important) differences that we'll talk about.
Lets start again with the decorator:

.. literalinclude:: workflow.py
    :lines: 6-7
    :language: python

Just like an activity, a workflow is identified by a name and a version - those
are needed when you want to start a new workflow. By default, a new workflow
will be queued to its default task list but that can be overridden latter. One
or more workers can then pull workflows from the task list and execute them.
There are a bunch of other defaults that can be set with this decorator like
the total duration this workflow can run before it times out. For now we'll
just go with the defaults.

.. literalinclude:: workflow.py
    :lines: 7-12
    :language: python

Here we define proxies for each activity we created earlier. A proxy is a
callable that will schedule an activity when called, passing all the arguments
it received. We set the name and the version of a proxy to the corresponding
activity. We can also override any value set in the activity decorator like we
did for the ``start_to_close`` value.

.. literalinclude:: workflow.py
    :lines: 14-25
    :language: python
    :linenos:

The ``run`` method contains the activity coordination code. Let's treat the
activity proxies as regular methods and see what happens here. Except for the
mysterious ``colors.result()`` call everything should be familiar: download,
resize and store an image in a temporary file, download the image again and
compute the predominant color and based on the color and move the temporary
file to a predefined location. Lastly, return the path where the image can be
found.

So now that we know what this workflow does lets see how it does it. The first
important thing to realize is that *each proxy call is asynchronous*. The call
only registers the activity to be scheduled (without necessarily scheduling it
yet) and returns in an instant. Because of this the return value of a proxy
call is a placeholder for the actual computation. You can pass this placeholder
to other activities as it is but if you need to access its value inside the
workflow you need to call the ``.result()`` method on it.

The second thing you need to know is that the ``run`` method doesn't actually
run for the entire duration of the workflow execution. It will run multiple
times and only for short periods of time. Actually, after the initial run when
the workflow is started, it will run every time an activity either finishes,
produces an error or times out. Each time the ``run`` method is invoked it
starts executing the code from the beginning, replacing the proxy calls with
different types of placeholders based on the most recent state of the
activities. A placeholder can either contain a result if the activity finished
successfully, an error if there was a problem or the activity timed out or
nothing at all if the activity is still running. While ``run`` the method is
running Flowy registers all the proxy calls and looks for new ones that receive
as arguments only constants or placeholders that contain results. All the proxy
calls identified as new are then used to schedule new activities.

But enough with the blabber, lets see an execution timeline of the workflow to
get a better understanding on how things work.


How Workflows Are Executed
--------------------------

If there is a single activity worker running, tracking the execution of the
workflow is not very exciting - everything will run just as if the code would
be synchronous. So lets consider there are multiple activity workers running,
each ready to pull and execute activities.

.. literalinclude:: workflow.py
    :lines: 14-17
    :language: python
    :linenos:

The first execution of the ``run`` method will happen sometime after the
workflow is scheduled, when one of the workflow runners is free. The first two
recorded proxy calls are those on lines 2 and 3 and both of them will return a
placeholder. Soon after that, the ``.result()`` method is called on the
``colors`` placeholder. When you access a result inside the workflow there are
two things that can happen (actually three, but don't worry about that): you
can get back the actual result if the activity finished successfully or
completely interrupt the execution of the ``run`` method otherwise. In this
case the ``run`` method is interrupted because the ``sum_colors`` activity
didn't finish - actually it wasn't even scheduled yet. Next, all the registered
proxy calls that depend only on values known at runtime or placeholders of
other finished activities are used to schedule their corresponding activities.
In our case both proxies are called with values known at runtime so both
activities get scheduled at the same time. The workflow runner is now free to
to process other workflow decisions.

As soon as one of the two activities finishes running a new decision will be
needed. One of the available workflow workers will execute the ``run`` method
again but this time the placeholders returned by the proxy calls will be
slightly different.

There are two possible continuations, based on which activity is the first one
to finish:

    1. ``resize`` is the first to finish: ``colors.result()`` still interrupts
       the execution, no new proxy calls are registered and thus no new
       activities are scheduled.
    2. ``sum_colors`` is the first to finish: ``colors.result()`` returns the
       actual activity result, the rest of the code is executed, a new proxy
       call for ``rename`` is registered but no activity is scheduled because
       the call depends on the value of ``resize`` which is unavailable.

As we can see, the 2nd time the ``run`` method is executed nothing will happen.
That's because we need both activities to finish in order to do progress. For
the 3rd decision we know both activity results for ``resize`` and
``sum_colors`` will be available and the ``rename`` activity is finally
scheduled. The last decision will complete the workflow execution as there are
no activities running and no new proxy calls detected.

.. warning::

    Because the ``run`` method is invoked multiple times for a single workflow
    execution it's very important that execution flow inside this method is
    deterministic. This means inside the workflow any conditional statement
    should only test input data or activity results. Code that schedules
    different activities based on time, random values or external data that may
    change will corrupt the workflow execution.


Running the Workflow Worker
---------------------------

Now that we know how the workflows are executed, lets start a few workflow
worker processes.  Like with the activity workers you can start multiple
processes and have the decision work distributed between them.  All we have to
do is execute the ``workflow.py`` file passing the Boto authentication
environment variables::

    (flowytutorial)$ AWS_ACCESS_KEY_ID=<your key> AWS_SECRET_ACCESS_KEY=<your secret> python workflows.py

You can schedule a workflow manually from the command line like so::

    (flowytutorial)$ AWS_ACCESS_KEY_ID=<your key> AWS_SECRET_ACCESS_KEY=<your secret> python -m flowy.swf flowytutorial imagecateg 1 http://www.jpeg.org/images/blue_large_01.jpg

.. seealso::

    :ref:`decision`
        In depth documentation on writing workflows.
    `Amazon SWF Developer Guide`_
        Provides a conceptual overview of Amazon SWF and includes detailed development instructions for using the various features.


Is This Useful?
---------------

Let's review some of the more important properties of the workflow we created:

    * There is no single process running for the entire duration of the
      workflow - this makes it possible to write workflows that run for very
      long periods of time without the fear of losing the progress.
    * Grouping activities on different task lists allows you to scale parts of
      the workflow independently (for example having two task lists for video
      and image processing).
    * Because of the diversity in timeout configuration and customizable
      retrying or manual error handling (which we haven't touched much in this
      tutorial) the workflow is very robust. We can restart all the activity
      and workflow workers at the same time, regardless of the state they are
      in and as soon as any of them is back up the workflow will happily
      continue its progress.


Next Steps
----------

Before diving in the in depth documentation section you should probably try to
experiment on your own with the code from the tutorial. Here are some ideas you
may try:

    * Open the Amazon console and inspect the history of your workflow
      execution.
    * While the workflow is running try restarting some workers and see what
      happens.
    * Try to make the activities raise some exceptions and see how it affects
      the workflow execution.
    * Spread the activities between two different task lists and scale them
      idependently.

While we are using Flowy to run a large number of workflows in production
systems it's still a new project. There are areas that can be improved and many
new ideas to experiment with. We'd love to hear from you and we could really
use your help. If you liked the turorial, have questions, found a bug or a very
cool idea, you can find us `on github`_.


.. _pillow: http://pillow.readthedocs.org/
.. _these steps: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-register-domain-console.html
.. _boto: http://boto.readthedocs.org/
.. _register_domain: http://boto.readthedocs.org/en/latest/ref/swf.html#boto.swf.layer1.Layer1.register_domain
.. _Amazon SWF Developer Guide: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-welcome.html
.. _on github: http://github.com/pbs/flowy/
