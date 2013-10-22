from flowy.client import workflow_client

workflow_client.schedule_on('SeversTest', 'prime_task_list', 'MyPrime', 2)
