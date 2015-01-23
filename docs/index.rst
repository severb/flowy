Flowy Docs
==========


`Flowy`_ is a library for building and running distributed, asynchronous
workflows built on top of `Amazon's SWF`_. Flowy deals away with the spaghetti
code associated with orchestrating complex workflows. It is ideal for
applications that have to deal with multi-phased batch processing, media
encoding, long-running tasks or background processing.

A toy map-reduce workflow with Flowy looks like this::

    @workflow(version='0.1-example')
    class SumSquares(Workflow):

            square = ActivityProxy(name='Square', version='0.1')
            sum = ActivityProxy(name='Sum', version='0.1')

            def run(self, n=5):
                    squares = map(self.square, range(n))
                    return reduce(self.sum, self.all(squares))

In the above example we compute the sum of the squares for a range of numbers
with the help of two activities (which make up a workflow): one that computes
the square of a number and one that sums up two numbers. Flowy will figure out
the dependencies between the activities so that the summing of the squares will
happen as soon as any two results of the squaring operation are available and
continue until everything is added together.

Before you start you should read the :doc:`introduction`. It explains important
concepts about the execution model of the workflows. Next, you should follow the
:doc:`tutorial`. It provides a narrative introduction of the most important
features of Flowy and a complex example of a workflow.


Installation
------------

Flowy is available at the Python Package Index site. To install it use `pip`_::

    pip install flowy


.. include:: changelog.rst

.. include:: roadmap.rst


.. toctree::
    :maxdepth: 2
    :hidden:

    introduction

    tutorial
    cookbook

    errors
    transport
    options

    versioning
    production
    faq
    contribute

    changelog

    reference


.. _Flowy: http://github.com/severb/flowy/
.. _Amazon's SWF: http://aws.amazon.com/swf/
.. _pip: http://www.pip-installer.org/
