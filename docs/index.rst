.. _index:

==========================
Flowy Amazon SWF Framework
==========================

Amazon Simple Workflow Service (Amazon SWF) provides a powerful and flexible
way for developers to implement distributed asynchronous workflow applications.
The AWS Flow Framework is a programming framework that simplifies the process
of implementing a distributed asynchronous application while providing all the
benefits of Amazon SWF. It is ideal for implementing applications to address a
broad range of scenarios including business processes, media encoding,
long-running tasks, and background processing.

With the :app:`Flowy` Framework, you can focus on implementing your workflow
logic.  Behind the scenes, the framework uses the scheduling, routing, and
state management capabilities of Amazon SWF to manage your workflow's execution
and make it scalable, reliable, and auditable. AWS Flow Framework-based
workflows are highly concurrent. The workflows can be distributed across
multiple components, which can run as separate processes on separate computers
and be scaled independently. The application can continue to progress if any of
its components are running, making it highly fault tolerant.

Here is one of the simplest :app:`Flowy` applications you can make:

.. literalinclude:: tutorial/simple_example.py

Contents:
=================
.. toctree::
   :maxdepth: 2

   installation
   tutorial

API Documentation
=================
.. toctree::
   :maxdepth: 1

   client
   workflow
   activity


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
