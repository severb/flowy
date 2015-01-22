Changelog
=========


Next Release
------------

The release date has still to be determined.

* Move public API components in ``flowy`` module.
* Add ``wait_for`` method on the ``SWFWorkflow`` class. It can be used to
  wait for a task to finish, similar with calling ``.result()`` but without the
  possibility of raising an exception even if error handling is on.
* Change ``first_n`` and ``all`` to return an iterator that can be used to
  iterate over the finished tasks in order. When the iterator is exhausted and
  more tasks are needed, the workflow is suspended.
* Remove ``fail()`` from the public API of tasks.
* Remove ``schedule_activity()`` and ``schedule_workflow()`` from the public
  API of the workflows.
* Fail the execution on arguments/results serialization/deserialization errors.
* Lazily serialize the proxy arguments, only once, at schedule time.

