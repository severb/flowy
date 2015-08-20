Flowy Docs
==========

`Flowy`_ is a workflow modeling and execution library. A workflow is a process
composed of independent and interdependent tasks. The independent tasks can be
concurrent and can run in parallel on many machines. Flowy uses
single-threaded Python code to model workflows. It infers the concurrency by
building the task dependency graph at run-time. A workflow engine, like
`Amazon's SWF`_, handles the task scheduling and routing. The engine also
stores task results and a history of the entire workflow execution. An open
source alternative to Amazon SWF is also available as part of the
`Eucalyptus`_ project.

Modeling workflows with Python code is easy and familiar. It also gives the
user great flexibility without sacrificing the readability. A toy example
workflow, with Flowy, looks like this:

.. testsetup:: example

   import time
   import flowy
   from concurrent.futures import ThreadPoolExecutor

   # The default executor uses multiple processes. This means it tries to
   # pickle the activities to pass them around. This fails for doctests.

   class LocalWorkflow(flowy.LocalWorkflow):
       def __init__(self, w):
           super(LocalWorkflow, self).__init__(w, executor=ThreadPoolExecutor)

   flowy.LocalWorkflow = LocalWorkflow


.. testcode:: example

   def sum_activity(a, b):
       time.sleep(1)
       return a + b

   def square_activity(n):
       time.sleep(1)
       return n ** 2

   class ExampleWorkflow(object):
       def __init__(self, square, sum):
           self.square = square
           self.sum = sum

       def __call__(self, a, b):
           a_squared = self.square(a)
           b_squared = self.square(b)
           return self.sum(a_squared, b_squared)

   w = flowy.LocalWorkflow(ExampleWorkflow)
   w.conf_activity('square', square_activity)
   w.conf_activity('sum', sum_activity)
   print(w.run(2, 10))

.. testoutput:: example
   :hide:

   104


Getting Started
---------------

Flowy is available on the Python Package Index site. To install it use
`pip`_::

    pip install flowy

Next, you should read the :doc:`tutorial`. It provides a narrative introduction
of the most important features of Flowy. It also shows how to run a workflow on
different engines.


.. include:: changelog.rst


.. toctree::
    :maxdepth: 2
    :hidden:

    tutorial
    changelog


.. _Flowy: http://github.com/severb/flowy/
.. _Amazon's SWF: http://aws.amazon.com/swf/
.. _Eucalyptus: http://eucalyptus.com
.. _pip: http://pip-installer.org/
