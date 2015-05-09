Next Release
------------

* A large and backward-incompatible rewrite.
* Added a local backend that can run workflows on a single machine using
  multiple threads or processes. This is very handy for local development and
  quick prototypes.
* Added workflow execution tracing and visualization as dot graphs.
* Task results use transparent proxy objects so that the workflow code can be
  run as sequential, single threaded, Python code.
* The workflow configuration is external, the same code can be configured to
  run on different backends.
