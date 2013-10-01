from pyswf.client import WorkflowStarter


workflow_starter = WorkflowStarter.for_domain('SeversTest', 'prime_task_list')
workflow_starter.start('MyPrimeParallel', 1, m=5)
