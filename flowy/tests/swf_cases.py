from flowy import SWFWorkflow
from flowy import SWFWorkflowWorker

from flowy.tests.workflows import *


NoTaskWorkflow = SWFWorkflow(version=1)
ClosureWorkflow = SWFWorkflow(version=1)
ArgumentsWorkflow = SWFWorkflow(version=1)
DependencyWorkflow = SWFWorkflow(version=1)
DependencyWorkflow.conf_activity('task', version=1)
ParallelWorkflow = SWFWorkflow(version=1)
ParallelWorkflow.conf_activity('task', version=1)
ParallelWorkflowRL = SWFWorkflow(name='ParallelRL', version=1, rate_limit=3)
ParallelWorkflowRL.conf_activity('task', version=1)
UnhandledExceptionWorkflow = SWFWorkflow(version=1)
SingleActivityWorkflow = SWFWorkflow(version=1)
SingleActivityWorkflow.conf_activity('task', version=1)
SAWorkflowCustomTimers = SWFWorkflow(name='SACustomTimers', version=1)
SAWorkflowCustomTimers.conf_activity(
    'task', version=1, heartbeat=10, schedule_to_start=11,
    schedule_to_close=12, start_to_close=13, task_list='TL',)


worker = SWFWorkflowWorker()
worker.register(NoTaskWorkflow, NoTask)
worker.register(ClosureWorkflow, Closure)
worker.register(ArgumentsWorkflow, Arguments)
worker.register(DependencyWorkflow, Dependency)
worker.register(ParallelWorkflow, Parallel)
worker.register(ParallelWorkflowRL, Parallel)
worker.register(UnhandledExceptionWorkflow, UnhandledException)
worker.register(SingleActivityWorkflow, SingleActivity)
worker.register(SAWorkflowCustomTimers, SingleActivity)


cases = [{
    'name': 'NoTask',
    'version': 1,
    'input_args': [10],
    'expected': {
        'finish': 10,
    },
}, {
    'name': 'NoTask',
    'version': 1,
    'input_args': ['abc'],
    'expected': {
        'finish': 'abc',
    },
}, {
    'name': 'NoTask',
    'version': 1,
    'input_args': [[1, 2, 3]],
    'expected': {
        'finish': [1, 2, 3],
    },
}, {
    'name': 'Closure',
    'version': 1,
    'input_args': [1],
    'expected': {
        'finish': 1,
    },
}, {
    'name': 'Arguments',
    'version': 1,
    'input_args': ['a', 'b'],
    'expected': {
        'finish': ['a', 'b', 1, 2],
    },
}, {
    'name': 'Arguments',
    'version': 1,
    'input_args': ['a', 'b', 'c'],
    'expected': {
        'finish': ['a', 'b', 'c', 2],
    },
}, {
    'name': 'Arguments',
    'version': 1,
    'input_args': ['a', 'b'],
    'input_kwargs': {'d': 'd'},
    'expected': {
        'finish': ['a', 'b', 1, 'd'],
    },
}, {
    'name': 'Dependency',
    'version': 1,
    'input_args': [5],
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
                'input_args': [0],
            },
        ],
    },
}, {
    'name': 'Dependency',
    'version': 1,
    'input_args': [5],
    'results': {
        'task-0-0': 1,
        'task-1-0': 2,
    },
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-2-0',
                'name': 'task',
                'version': 1,
                'input_args': [2],
            },
        ],
    },
}, {
    'name': 'Dependency',
    'version': 1,
    'input_args': [5],
    'results': {
        'task-0-0': 1,
    },
    'errors': {
        'task-1-0': 'err!',
    },
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'Parallel',
    'version': 1,
    'input_args': [5],
    'results': {
        'task-0-0': 1,
        'task-1-0': 2,
        'task-2-0': 3,
        'task-3-0': 4,
        'task-4-0': 5,
    },
    'expected': {
        'finish': [1, 2, 3, 4, 5],
    },
}, {
    'name': 'Parallel',
    'version': 1,
    'input_args': [4],
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
                'input_args': [0],
            },
            {
                'type': 'activity',
                'call_key': 'task-1-0',
                'name': 'task',
                'version': 1,
                'input_args': [1],
            },
            {
                'type': 'activity',
                'call_key': 'task-2-0',
                'name': 'task',
                'version': 1,
                'input_args': [2],
            },
            {
                'type': 'activity',
                'call_key': 'task-3-0',
                'name': 'task',
                'version': 1,
                'input_args': [3],
            },
        ],
    },
}, {
    'name': 'Parallel',
    'version': 1,
    'input_args': [4],
    'running': [
        'task-0-0',
        'task-2-0',
        'task-3-0',
    ],
    'results': {
        'task-1-0': 2,
    },
    'expected': {
        'schedule': [],
    },
}, {
    'name': 'ParallelRL',
    'version': 1,
    'input_args': [4],
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
                'input_args': [0],
            },
            {
                'type': 'activity',
                'call_key': 'task-1-0',
                'name': 'task',
                'version': 1,
                'input_args': [1],
            },
            {
                'type': 'activity',
                'call_key': 'task-2-0',
                'name': 'task',
                'version': 1,
                'input_args': [2],
            },
        ],
    },
}, {
    'name': 'UnhandledException',
    'version': 1,
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'SingleActivity',
    'version': 1,
    'errors': {
        'task-0-0': 'err!',
    },
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'SACustomTimers',
    'version': 1,
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
                'task_list': 'TL',
                'schedule_to_start': 11,
                'schedule_to_close': 12,
                'start_to_close': 13,
                'heartbeat': 10,
            },
        ],
    },
}]
