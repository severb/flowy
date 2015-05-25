Flowy Docs
==========

`Flowy`_ is a library for building and running distributed, asynchronous
workflows built on top different backends (such as `Amazon's SWF`_). Flowy
deals away with the spaghetti code associated with orchestrating complex
workflows. It is ideal for applications that have to deal with multi-phased
batch processing, media encoding, long-running tasks, and/or background
processing.

Flowy can model any execution topology. The same code can also run without
Flowy as a single-threaded sequential implementation, making it easy to test
and understand. A toy map-reduce workflow with Flowy running on the local
backend using multiple processes might look like this::

    def sum_activity(n1, n2):
        return n1 + n2

    def square_activity(n):
        return n ** 2

    class SumSquares(object):
        def __init__(self, square, sum):
            self.square = square
            self.sum = sum

        def __call__(self, n=5):
            squares = map(self.square, range(n))
            return parallel_reduce(self.sum, squares)

    if __name__ == '__main__':
        # Run it in parallel on multiple processes
        w = LocalWorkflow(SumSquares)
        w.conf_activity('square', square_activity)
        w.conf_activity('sum', sum_activity)
        print(w.run())

        # Or sequentially, without Flowy
        ss = SumSquares(square_activity, sum_activity)
        print(ss())

The above workflow example computes the sum of the squares for a range of
numbers with the help of two activities: one that computes the square of a
number and one that sums up two numbers. Flowy will find the dependencies
between the activities so that the summing and the squaring will happen as soon
as possible while maximizing parallelization. The same code can be
configured differently to run across many machines using a remote backend
like Amazon SWF or Eucalyptus as an open-source alternative.


Getting Started
---------------

Flowy is available on the Python Package Index site. To install it use `pip`_::

    pip install flowy

Next, you should read the :doc:`tutorial`. It provides a narrative
introduction of the most important features of Flowy and a complex workflow
example running on different backends.


.. include:: changelog.rst
.. include:: roadmap.rst


.. toctree::
    :maxdepth: 2
    :hidden:

    tutorial

    swf/index.rst

    cookbook
    errors
    transport

    faq
    contribute
    changelog
    reference


.. _Flowy: http://github.com/severb/flowy/
.. _Amazon's SWF: http://aws.amazon.com/swf/
.. _pip: http://www.pip-installer.org/
