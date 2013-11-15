.. _tutorial:

========
Tutorial
========

In this section, we will build a simple workflow from start to finish, using
:app:`Flowy`, explaining in detail how the framework works. It assumes you
have :app:`Flowy` and all its dependencies installed. If you don't, go to the
:ref:`installation` section.

First things first: we need a project. In order to demonstrate several key
components of the framework we will build a workflow for a video streaming
application.

.. note:: In this tutorial our focus will be on creating the workflow and not
   the video streaming app itself. The point of said app is to illustrate
   how several logical components can interract within a workflow constructed
   with :app:`Flowy`.

The basic idea of our mock application is that users upload video files to our
servers, and the people can browse, view or rate them. Think YouTube.  As far
as our app's backend is concerned we can identify the following logical flow:

#TODO:
Graph showing a basic sequential flow. Upload -> Transcoding -> Thumbnail
Generation -> Metadata Processing etc.

#TODO:
Show parallelised version of the graph above?

As you can see, our flow is divided into several units of work, each
corresponding to an activity within :app:`Flowy`, so without further adieu, we
can go ahead and create an activity for the video transcoding.

Creating our first Activity
---------------------------

.. literalinclude:: transcoding.py
   :linenos:

So inserting this code into a Python script named transcoding_activity.py
grants us our first activity. Let's examine it piece-by-piece.

Imports
~~~~~~~

The activity above uses the following set of import statements:

.. literalinclude:: transcoding.py
   :lines: 1-2

The script imports the :class:`~flowy.activity.Activity` class from the
:mod:`flowy.activity` module. Our activity must inherit this class. We also
import ``activity_client`` which is an instance of
:class:`~flowy.client.ActivityClient` which we will use to decorate our
activity, specifying the activity's name, version and task list (several other
options can be specified here as well). Activities can also be registered
manually with the :meth:`~flowy.client.ActivityClient.register` method.

.. seealso:: For a full list of options that can be specified when registering
   an activity using the decorator, or manually, please see
   :meth:`flowy.client.ActivityClient.register`.

Defining the activity
~~~~~~~~~~~~~~~~~~~~~

Defining our activity class is done in the following manner:

.. literalinclude:: transcoding.py
   :language: python
   :lines: 4-6

When decorating the activity class with ``activity_client`` we must specify
a name, version, and a task list to register the activity with. The activity
will be scheduled on the task list called ``transcoding_list``, and only
processes that poll for activities on that task list will receive it for
processing.  Our activity class must also inherit
:class:`~flowy.client.ActivityClient`.

.. literalinclude:: transcoding.py
   :language: python
   :lines: 8-14

The :meth:`~flowy.activity.Activity.run` method is the heart of our activity.
That is where we must implement the activity logic itself. The
``some_transcoding_method`` method is just a placeholder for some business
logic however complex it might be.

Finally, we have:

.. literalinclude:: transcoding.py
   :language: python
   :lines: 16-17

What this does, is start the main activity polling loop on the
``transcoding_list`` task list. Notice, the same task list was specified when
creating our activity.

In the diagram above, we identified 3 activities that we need to define and
implement. The activities that process the metadata and generate thumbnails can
be created in the same exact manner we created the ``Transcoding`` activity.

.. note:: As of now, we will presume we have 3 activities implemented:
   Transcoding, ThumnailGenerator and MetadataProcessing.

Creating our first Workflow
---------------------------

.. literalinclude:: workflow.py
   :linenos:

Let's put the script above in a file called ``video_processing_workflow.py``
and examine it piece-by-piece.

Imports
~~~~~~~

The workflow above uses the following set of import statements:

.. literalinclude:: workflow.py
   :language: python
   :lines: 1-3

The script imports the :class:`~flowy.workflow.Workflow` class from the
:mod:`flowy.workflow` module. Our Workflow must inherit this class and
implement the :meth:`~flowy.workflow.Workflow.run` method. We also import the
:class:`~flowy.workflow.ActivityProxy` class which we will use to represent our
previously implemented activities. Last but not least, we import
``workflow_client``, an instance of :class:`~flowy.client.WorkflowClient`,
which we will use to decorate our workflow, specifying the
workflow's name, version, and task list.

Defining the workflow
~~~~~~~~~~~~~~~~~~~~~

We define a workflow in the following way:

.. literalinclude:: workflow.py
   :language: python
   :lines: 4-6

When decorating the workflow class with ``workflow_client`` we must specify a
name, version and a default task list to register the workflow with. The
workflow can also be manually registered using
:meth:`~flowy.client.WorkflowClient.register`.  The specified task list will be used
in case one is not specified when starting the workflow. Our workflow class
must also inherit :class:`~flowy.workflow.Workflow`.

.. seealso:: For a full list of options that can be specified when registering
   a workflow using the decorator, or manually, please see
   :meth:`flowy.client.WorkflowClient.register`.

We define activities within our workflow as such:

.. literalinclude:: workflow.py
   :language: python
   :lines: 8-10

When initializing :class:`~flowy.workflow.ActivityProxy` classes, the names and
versions provided must match the names and versions we set when defining our
activities.

Finally, we arrive to the workflow implementation:

.. literalinclude:: workflow.py
   :language: python
   :lines: 11-17

As you can see the actual workflow implementation is pretty straight-forward,
with a few exceptions: the method :meth:`~flowy.workflow.MaybeResult.result`.
Whenever we want to access the result of an activity, we must do so through the
aforementioned method. This method is the only synchronization primitive
available in :app:`Flowy`, and its usage will be explained later on. An
activity result must be something that can be serialized using
:meth:`~flowy.activity.activity.serialize_activity_result`. The default
serialization method uses JSON. For activities returning more complex
datatypes, simply override the serialization and deserialization methods.

.. literalinclude:: workflow.py
   :language: python
   :lines: 19-20

The line above starts the main decision polling loop on the ``upload_list``
task list.

Concurrency
-----------

Let's take another look at our workflow implementation:

.. literalinclude:: workflow.py
   :language: python
   :lines: 11-17

The script is written in plain, sequential Python. First, the ``Transcoding``
activity is executed, then the ``ThumbnailGenerator`` activity, and after that
their results are used by the ``MetadataProcessing`` activity. No
parallelization indications are given whatsoever. However, :app:`Flowy`
automatically detects that the first two activities can be run in parallel, so
it schedules them at the same time, thus revealing one of the most interesting
features of the framework. Since the ``MetadataProcessing`` activity requires
that both previous activities be finished for it to start, the workflow
execution halts until both the ``Transcoding`` and the ``ThumnailGenerator``
activities are finished, only then starting ``MetadataProcessing``.
This is achieved using the :meth:`~flowy.workflow.MaybeResult.result` method,
:app:`Flowy`'s only syncronization primitive.

.. note:: When we need the actual result returned by an activity we must use the
   :meth:`~flowy.workflow.MaybeResult.result` method. As mentioned before, this will
   block the workflow execution until a result is available.

So what happens if we add a new activity called ``AddSubstitles`` after the
``MetadataProcessing`` activity?

.. literalinclude:: concurrency_bad_example.py
   :language: python
   :emphasize-lines: 7,14

Notice the ``AddSubtitles`` activity does not depend on any other activities,
so one might expect it to start at the same time the ``Transcoding`` and
``ThumnailGenerator`` activities are started. However, by calling
:meth:`~flowy.workflow.MaybeResult.result` in the arguments given to the
``MetadataProcessing`` activity, :app:`Flowy` will synchronize the
``Transcoding`` and ``ThumbnailGenerator`` activities, scheduling any other
activities only after these are finished. The ``AddSubtitles`` activity will
therefore be scheduled together with the ``MetadataProcessing`` activity.

.. note:: When activities depend on other activities' results, the person
   implementing the workflow is responsable for calling activities in a way it
   makes most sense, so :app:`Flowy` can paralellize as many activities as
   possible.

Returning to our example above, if the ``AddSubtitles`` activity would have
been called before ``MetadataProcessing``, :app:`Flowy` would have scheduled it
together with the ``Transcoding`` and ``ThumbnailGenerator`` activities, which
would have been optimal.

Configuring activities
----------------------

Our imaginary application is getting more complex, and we need to make our
activities better tailored for our specific needs. Since the ``Transcoding``
activity can take a long long time (days, weeks, even months) to complete, we
want it to report progress with every transcoded megabyte (which should take at
most 30 seconds), so in case it gets stuck for whatever reason (disk space,
deadlock, etc), we can handle the failiure accordingly. We also want to ensure
our activity finishes in 30 minutes (1800 seconds), and in case it doesn't, we
will transcode the video using an activity that will be processed on a faster
machine. We also want to make sure our activity will be started within 3
minutes (120 seconds) from the time it was scheduled.

Luckily, configuring activities is easy and controllable. There are 3 ways to
configure an activity:

1. When defining the activity, through the decorator:

.. literalinclude:: configurations.py
   :language: python
   :lines: 1-9
   :emphasize-lines: 1,2

2. When defining a :class:`~flowy.workflow.ActivityProxy` in the workflow:

.. literalinclude:: configurations.py
   :language: python
   :lines: 12-24
   :emphasize-lines: 4,5,6,7,8,9,10,11

3. When implementing the workflow itself, via a special context manager:

.. literalinclude:: configurations.py
   :language: python
   :lines: 27-38
   :emphasize-lines: 8,9

The 3 different scopes that are available for specifying certain activity
options have a well defined hierarchy: options set via the third method will
rewrite any options that were set either when defining the ``ActivityProxy`` or
via the decorator. The settings specified when defining the ``ActivityProxy``
will also rewrite any and all settings specified with the decorator.

Error handling
--------------
