Flowy Library Documentation
===========================


`Flowy`_ is a library that makes it easy to write distributed asynchronous
workflows. Using Flowy you can write many dependend jobs and put them in a
queue for asynchronous execution but without the spaghetti code. It uses
`Amazon SWF`_ as a backend. It's ideal for applications that deal with media
encoding, long-running tasks or background processing.

A toy map-reduce workflow using Flowy looks like this::

    @workflow(version='0.1-example')
    class SumSquares(Workflow):

            square = ActivityProxy(name='Square', version='0.1')
            sum = ActivityProxy(name='Sum', version='0.1')

            def run(self, n=5):
                    squares = map(self.square, range(n))
                    return self.sum(*squares)

Before you start you should read the :ref:`SWF Introduction <introduction>`. It
explains important concepts about the execution model of the workflows. Next,
you should follow :ref:`the tutorial <tutorial>`. It provides a narrative
introduction of the most important features of Flowy and a complete example of
a workflow.


Installation
------------

Flowy is available on the Python Package Index. To install it use `pip`_::

    pip install flowy


Tutorial
--------

.. toctree::
    :maxdepth: 2

    tutorial/tutorial


In Depth
--------

.. toctree::
    :maxdepth: 2

    indepth/activity
    indepth/workflow
    indepth/error
    indepth/settings
    indepth/transport
    indepth/versioning
    indepth/production
    indepth/contribute



.. _Flowy: http://github.com/pbs/flowy/
.. _Amazon SWF: http://aws.amazon.com/swf/
.. _pip: http://www.pip-installer.org/
