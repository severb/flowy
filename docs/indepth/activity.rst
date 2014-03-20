.. _activity:

Activity Task
=============

An activity task is the smallest computational part of a workflow. You can
think of it as a function: it can get an input, does some processing and may
return something back. The easiest way to implement an activity is by
subclassing the ``Activity`` class and overriding the ``run`` method. This
provides some convenience for you like automatic error handling, input
deserialization and result serialization.

A very simple echo activity that returns the input it received looks like
this::

    class Echo(Activity):
        def run(self, value):
            return value


Activity I/O
------------

Just as any regular Python method you can provide some default values for your
arguments making them optional later when the workflow schedules activities to
run. You can also receive variable arguments both positional or keyword.
Actually, you can use valid Python method signature for your activities::

    class Echo(Activity):
        def run(self, value=None, *args, **kwargs):
            return (value,) + tuple(args) + tuple(kwargs.values())

There are some limitations on the values that can be passed as arguments to the
activity or returned back as results: they must be JSON serializable. That's
because the default transport implementation uses JSON to pass data to and back
from Amazon. You can change this by overriding the ``_serialize_result`` and
``_deserialize_arguments`` methods but this is rarely enough as the workflow
must also be aware of this changes. For a complete explanation on how to change
the transport protocol see :ref:`transport`.
