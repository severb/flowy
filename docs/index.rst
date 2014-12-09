Flowy Documentation
===================


`Flowy`_ is a library for distributed asynchronous workflows built on top of
`Amazon SWF`_. With Flowy you can write many dependend jobs and put them in a
queue for asynchronous execution but without the spaghetti code.  It's ideal
for applications that deal with media encoding, long-running tasks or
background processing.

A toy map-reduce workflow with Flowy looks like this::

    @workflow(version='0.1-example')
    class SumSquares(Workflow):

            square = ActivityProxy(name='Square', version='0.1')
            sum = ActivityProxy(name='Sum', version='0.1')

            def run(self, n=5):
                    squares = map(self.square, range(n))
                    return self.sum(*squares)

Before you start you should read the :doc:`introduction`. It explains important
concepts about the execution model of the workflows. Next, you should follow
the :doc:`tutorial`. It provides a narrative introduction of the most important
features of Flowy and a complete example of a workflow.


Installation
------------

Flowy is available on the Python Package Index. To install it use `pip`_::

    pip install flowy


.. include:: changelog.rst


.. toctree::
    :maxdepth: 2
    :hidden:

    introduction

    tutorial

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
.. _Amazon SWF: http://aws.amazon.com/swf/
.. _pip: http://www.pip-installer.org/
