from flowy.client import WorkflowClient


workflow_client = WorkflowClient.for_domain('SeversTest', 'prime_task_list')
workflow_client.schedule('MyPrime', 2)
