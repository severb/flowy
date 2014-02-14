Flowy Library Documentation
===========================


`Flowy`_ is a library that makes it easy to write distributed asynchronous
workflows. It uses `Amazon SWF`_ as a backend. It is ideal for applications
that deal with media encoding, long-running tasks or background processing.

A toy map-reduce workflow using Flowy looks like this::

    @workflow(name='add_squares', version='v1', task_list='my_list')
    class AddSquares(Workflow):

            square = ActivityProxy(name='square', version='v0.1')
            add = ActivityProxy(name='add', version=3)

            def run(self, n=5):
                    squares = map(self.square, range(n))
                    return self.add(*squares)

See the :ref:`tutorial <tutorial>` for a narrative introduction of the Flowy
features.


Installation
------------

Flowy is available on the Python Package Index - to install it use `pip`_::

    pip install flowy


Tutorial
--------

.. toctree::
    :maxdepth: 2

    tutorial/tutorial


In Depth
--------

.. toctree::
    :titlesonly:

    indepth/activity
    indepth/decision
    indepth/error
    indepth/settings
    indepth/transport
    indepth/production
    indepth/contribute



.. _Flowy: http://example.com/
.. _Amazon SWF: http://aws.amazon.com/swf/
.. _pip: http://www.pip-installer.org/


