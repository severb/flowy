.. _tutorial:

Tutorial
========

In this tutorial we'll create a workflow that resizes and classifies images
based on their predominant color. A typical workflow  uses one or more
activities that do the actual data processing. Here we'll use three activities:
one for image resizing, another one for predominant color computation and the
last one will move images from one place to another. The workflow
responsibility is coordinate these activities and pass data between them. Each
time an activity finishes its execution the workflow will decide what needs to
happen next. This decision can be one of the following: start or retry
activities, complete or abort the workflow execution.

But before, lets start by setting up the development environment and create a
new domain.


Setting up the environment
--------------------------

To isolate the requirements for this tutorial lets create a new virtual
environment and install all the dependencies::

    $ virtualenv /tmp/flowytutorial/
    $ source /tmp/flowytutorial/bin/activate
    (flowytutorial)$ pip install flowy requests pillow # a fork of PIL

After installation make sure `pillow`_ has JPEG support. You should see::

    --- JPEG support available


Registering a new domain
------------------------


Before implementing our first workflow we need to define a domain in the Amazon
SWF service that will host the workflow and all its activities. The domain is
like a namspace, only the workflows an activities registered under the same
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

TBD


.. _pillow: http://pillow.readthedocs.org/
.. _these steps: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-register-domain-console.html
.. _boto: http://boto.readthedocs.org/
.. _register_domain: http://boto.readthedocs.org/en/latest/ref/swf.html#boto.swf.layer1.Layer1.register_domain
