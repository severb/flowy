SWF Introduction
================

Before we start using Flowy lets get familiar with Amazon SWF. If you already
know how SWF works you can head straight to the Tutorial. If you never used SWF
before then you should continue reading because we'll be using a lot the
concepts and the terms we introduce here.

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

Here we can see the two different types of task lists in action:

* The activities task list is like your regular job queue, each activity
  represents a specific task, part of a workflow. The activity workers consume
  this task list, polling and executing activities in a never ending loop. You
  can have many workers of this type doing the polling and executing tasks in
  parallel.

* The decisions task list is  similar with the activities task list. You have
  some workers polling decisions and executing them in a never ending loop. The
  difference between a decision and an activity is that the decision doesn't do
  any actual processing. The decisions act as coordinators for activities. You
  can say that a workflow logic is the sum of all decisions needed for it. At
  the same time the actual computation of a workflow is the sum of all
  activities executed.


Polling activities
------------------

Lets focus on running activities now. We said earlier that activities are just
like your regular jobs in a job queue. But we didn't mention what happens after
an activity is executed. Where does the result go and how does that affect the
workflow?

.. figure:: imgs/swf_activities.png
   :align: center

   Polling and running activities

This is the entire lifetime of an activity. Lets go over it step by step:

1. The worker starts by long polling the SWF web service.
2. As soon as there is an activity waiting in the activity task list it will be
   sent to one (and only one) of the listening workers. *(We'll see later how an
   activity is added to a task list.)*
3. Each activity has two parts: an identity composed by a name and a version
   and some input data. After a worker retrieved an activity it uses the
   identity to locate the corresponding code and launches it passing the input
   data. Note that there is no code passed with the activity so the worker must
   know the activity code beforehand.
4. After the activity is executed the final result is sent back to SWF.
5. When the result is sent, a new decision will automatically be added in the
   decisions task list.
