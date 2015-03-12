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

Getting Started
---------------

Flowy is available at the Python Package Index site. To install it use `pip`_::

    pip install flowy

Next, you should read the :doc:`tutorial`. It provides a narrative
introduction of the most important features of Flowy and a complex workflow
example running on different backends.


.. include:: changelog.rst
.. include:: roadmap.rst


Flowy is an open source project backed by |3PG|_


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
.. |3PG| image:: http://www.3pillarglobal.com/wp-content/themes/base/library/images/logo_3pg.png
    :height: 25px
.. _3PG: http://3pillarglobal.com/
