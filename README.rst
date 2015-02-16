.. image:: https://travis-ci.org/severb/flowy.svg?branch=master
   :target: https://travis-ci.org/severb/flowy

.. image:: https://coveralls.io/repos/severb/flowy/badge.png?branch=master
   :target: https://coveralls.io/r/severb/flowy?branch=master

.. image:: https://landscape.io/github/severb/flowy/master/landscape.png
    :target: https://landscape.io/github/severb/flowy/master

.. image:: https://pypip.in/version/flowy/badge.png
   :target: https://pypi.python.org/pypi/flowy/

.. image:: https://pypip.in/download/flowy/badge.png
   :target: https://pypi.python.org/pypi/flowy/

.. image:: https://pypip.in/license/flowy/badge.png
   :target: https://pypi.python.org/pypi/flowy/

.. image:: https://pypip.in/format/flowy/badge.png
   :target: https://pypi.python.org/pypi/flowy/


Flowy Docs
==========

`Flowy`_ is a library for building and running distributed, asynchronous
workflows built on top different backends including `Amazon's SWF`_. Flowy
deals away with the spaghetti code associated with orchestrating complex
workflows. It is ideal for applications that have to deal with multi-phased
batch processing, media encoding, long-running tasks or background processing.

A toy map-reduce workflow with Flowy looks like this::

    wcfg = SWFWorkflowConfig(version=3, workflow_duration=60)
    wcfg.add_activity('sum', version=1)
    wcfg.add_activity('square', version=7, schedule_to_close=5)

    @wcfg
    class SumSquares(object):
        def __init__(self, square, sum):
            self.square = square
            self.sum = sum

        def run(self, n=5):
                squares = map(self.square, range(n))
                return parallel_reduce(self.sum, squares)

In the above example we compute the sum of the squares for a range of numbers
with the help of two activities (which make up a workflow): one that computes
the square of a number and one that sums up two numbers. Flowy will figure out
the dependencies between the activities so that the summing of the squares will
happen as soon as any two results of the squaring operation are available and
continue until everything is added together. The activities themselves can also
be implemented in Flowy as regular Python functions.
