import tchannel.sync

import flowy.cadence.client
import flowy.cadence.config
import flowy.cadence.worker

import workflows

# For simplicity, we'll use the same config for all workflows
# that have similar dependencies.

# Usually, defaults can go in the SWFWorkflowConfig, like default
# tasklist, etc. but these are not supported in Cadence yet.
no_args_config = flowy.cadence.config.SWFWorkflowConfig()


sum_only_config = flowy.cadence.config.SWFWorkflowConfig()
sum_only_config.conf_activity(
    'sum_activity', 1,
    name='sum',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)  # on timeout, retry twice with 0 and 1 seconds delay
                  # this can be any integer iterator
)

err_only_config = flowy.cadence.config.SWFWorkflowConfig()
err_only_config.conf_activity(
    'err_activity', 1,
    name='err',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)

sum_and_mul_config = flowy.cadence.config.SWFWorkflowConfig()
sum_and_mul_config.conf_activity(
    'sum_activity', 1,
    name='sum',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)
sum_and_mul_config.conf_activity(
    'mul_activity', 1,
    name='mul',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)


sum_mul_err_config = flowy.cadence.config.SWFWorkflowConfig()
sum_mul_err_config.conf_activity(
    'sum_activity', 1,
    name='sum',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)
sum_mul_err_config.conf_activity(
    'mul_activity', 1,
    name='mul',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)
sum_mul_err_config.conf_activity(
    'err_activity', 1,
    name='err',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)


subworkflows_config = flowy.cadence.config.SWFWorkflowConfig()
subworkflows_config.conf_activity(
    'compute_width', 1,
    name='sum',
    task_list='demotl',
    heartbeat=10,
    schedule_to_close=50,
    schedule_to_start=20,
    start_to_close=60,
    retry=(0, 1)
)
subworkflows_config.conf_workflow(
    'compute_length', 1,
    name='SumAndMulWorkflow',
    task_list='demotl',
    workflow_duration=120,
    decision_duration=10,
)


workflow_worker = flowy.cadence.worker.SWFWorkflowWorker()

# This is explicit registration.
# There's a better way to do it with decorators
# but just want to keep things clear for this demo.

workflow_worker.register(
    no_args_config,
    workflows.NoopWorkflow,
    version=1,
)

workflow_worker.register(
    sum_only_config,
    workflows.SimpleWorkflow,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.SumAndMulWorkflow,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.SumAndMulWorkflow2,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.SumAndMulWorkflow3,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.SumAndMulWorkflow4,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.ResultConditionalWorkflow,
    version=1,
)

workflow_worker.register(
    sum_mul_err_config,
    workflows.ImplicitErrorPropagationWorkflow,
    version=1,
)

workflow_worker.register(
    err_only_config,
    workflows.ExplicitErrorHandlingWorkflow,
    version=1,
)

workflow_worker.register(
    err_only_config,
    workflows.ExplicitResultDereferenceWorkflow,
    version=1,
)


workflow_worker.register(
    flowy.cadence.config.SWFWorkflowConfig(),
    workflows.UnhandledExceptionWorkflow,
    version=1,
)

workflow_worker.register(
    flowy.cadence.config.SWFWorkflowConfig(),
    workflows.RestartingWorkflow,
    version=1,
)

workflow_worker.register(
    subworkflows_config,
    workflows.Subworkflows,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.WaitForFirstWorkflow,
    version=1,
)

workflow_worker.register(
    sum_only_config,
    workflows.WaitForFirstNWorkflow,
    version=1,
)

workflow_worker.register(
    sum_and_mul_config,
    workflows.ParallelReduceWorkflow,
    version=1,
)



tchannel = tchannel.sync.TChannel('flowy')
c = flowy.cadence.client.SWFClient(tchannel)
workflow_worker.run_forever('demo', 'demotl', c)
