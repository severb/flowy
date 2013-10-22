from flowy.client import workflow_client

prime_w = workflow_client.scheduler_on(
    domain='SeversTest',
    name='MyPrime',
    version=2,
    task_list='prime_task_list',
)
prime_w()
