.. _tutorial:

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
   with :app:`Flowy`

The basic idea of our mock application is that users upload video files to our
servers, and the people can browse, view or rate them. Think YouTube.

As far as our app's backend is concerned we can identify the following logical
flow:

#TODO:
Graph showing a basic sequential flow. Upload -> Transcoding -> Thumbnail
Generation -> Metadata Processing etc.

#TODO:
Show parallelised version of the graph above?

As you can see, our flow is divided into several units of work, each
corresponding to an activity within :app:`Flowy`, so without further adieu, we
can go ahead and create an activity for the video transcoding.

Creating our first Activity:
----------------------------

.. code-block:: python

