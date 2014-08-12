SWF Introduction
================

Before we start using Flowy lets get familiar with what Amazon SWF is and how
it works. If you already know this stuff you can skip and head straight to the
Tutorial. If you never used SWF before then you should continue reading because
we'll be using a lot the concepts and the terms we introduce here.

SWF is a web service offered by Amazon as part of their Cloud Computing
Services. It's advertised as something that helps you build, run, and scale
background jobs that have parallel or sequential steps. A fully-managed state
tracker and task coordinator in the Cloud.

In simple words SWF is a web service over two different types of queues. In SWF
parlance the queues are called task lists because they have some special
behavior that queues don't have. But for now we're better of if we're thinking
of them as queues.


Polling tasks
-------------

One of the things that we usually do with queues is to pop values out. In SWF
we do this with long polling. Here's a diagram of this process:

.. figure:: imgs/swf_overview.png
   :align: center

   Polling tasks from the task lists using HTTP long polling
