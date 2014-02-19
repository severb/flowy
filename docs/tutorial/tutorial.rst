.. _tutorial:

Tutorial
========

In this tutorial we'll create a workflow that resizes and classifies images
based on their predominant color. A typical workflow uses one or more
activities that do the actual data processing. Here we'll use three activities:
one for image resizing, another one for predominant color computation and the
last one will move images from one place to another. The workflow
responsibility is coordinate these activities and pass data between them. Each
time an activity finishes its execution the workflow will decide what needs to
happen next. This decision can be one of the following: start or retry
activities, complete or abort the workflow execution.

But before, lets start by setting up the development environment and create a
new domain.


Setting Up the Environment
--------------------------

To isolate the requirements for this tutorial lets create a new virtual
environment and install all the dependencies::

    $ virtualenv /tmp/flowytutorial/
    $ source /tmp/flowytutorial/bin/activate
    (flowytutorial)$ pip install flowy requests pillow # a fork of PIL

After installation make sure `pillow`_ has JPEG support. You should see::

    --- JPEG support available


Registering a New Domain
------------------------

Before implementing our first workflow we need to define a domain in the Amazon
SWF service that will host the workflow and all its activities. The domain is
like a namspace, only the workflows and activities registered under the same
domain can see eachother. You can register a domain using the management
console and following `these steps`_ - just make sure you name it
*flowy_tutorial*. You can also register a domain using the `boto`_ library:
launch your Python interpreter with the following two environment variables
set::

        (flowytutorial)$ AWS_ACCESS_KEY_ID=<your key> AWS_SECRET_ACCESS_KEY=<your secret> python

And use the `register_domain`_ method like so:

        >>> from boto.swf.layer1 import Layer1
        >>> Layer1().register_domain('flowy_tutorial', 7)  # keep the run history for 1 week


Image Resizing Activity
-----------------------


Activities are the components of a workflow that do the actual processing. As
we'll see in the example below you implement an activity by overriding the
``run`` method of the ``Activity`` class. You can pass data in the activity
from the workflow and out from the activity to the workflow. Once we have a few
activities implemented we can spawn as many activity worker processes as we
need. Each worker will scan for all activities implemented and start listening
for things to do. As soon as a workflow schedules a new activity to run one of
the available worker will execute the activity code and report back the result.
Lets create our first activity, open a new file named ``activities.py`` and add
the following content:

.. literalinclude:: activities.py
    :linenos:
    :lines: 1-35
    :language: python


There is a lot going on in this activity so lets go over it line by line.
Skipping all the imports we get to the first interesting part which is the
``activity`` decorator:

.. literalinclude:: activities.py
    :lines: 12
    :language: python


The ``activity`` decorator has two different responsibilities. It registers
this activity implementation so that when we later spawn an activity worker the
activity is discovered. At the same time it provides some required properties
like the activity name, version and the default default task list. A workflow
can use the name and the version defined here to schedule a run of this
activity - we'll see exactly how later. The task lists control how tasks are
routed to activity worker processes. Whenever we spawn an activity worker
process we specify a task list to pull tasks from; we can even have many
workers pulling from the same list. The task list defined with the ``activity``
decorator only serves as a default - the workflow can force an activity to be
scheduled on a specific list. There are other things that can be specified with
this decorator like timeout values, error handling strategy and the number of
retries but for now we won't need any of those.


.. _pillow: http://pillow.readthedocs.org/
.. _these steps: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-register-domain-console.html
.. _boto: http://boto.readthedocs.org/
.. _register_domain: http://boto.readthedocs.org/en/latest/ref/swf.html#boto.swf.layer1.Layer1.register_domain
