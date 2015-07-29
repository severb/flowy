Next Release
------------

* A large and backward-incompatible rewrite.
* Added a local engine that can run workflows on a single machine using
  threads or processes. This is handy for local development and quick
  prototypes.
* Added workflow execution tracing and visualization, as dot graphs, for the
  local engine.
* Proxy objects replaced task results. This allows a workflow to run as single
  threaded Python code, without Flowy. It also makes testing more convenient.
* Moved the workflow configuration outside of the workflow code. This makes it
  easy to configure the same workflow to run on different engines.
