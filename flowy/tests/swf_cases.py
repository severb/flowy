from flowy import SWFWorkflow
from flowy import SWFWorkflowWorker

from flowy.tests.workflows import *


QuickReturnWorkflow = SWFWorkflow(version=1)
ClosureWorkflow = SWFWorkflow(version=1)
ArgumentsWorkflow = SWFWorkflow(version=1)
DependencyWorkflow = SWFWorkflow(version=1)
DependencyWorkflow.conf_activity('inc', version=1)
ParallelWorkflow = SWFWorkflow(version=1)
ParallelWorkflow.conf_activity('inc', version=1)
ParallelWorkflowRL = SWFWorkflow(name='ParallelRL', version=1, rate_limit=3)
ParallelWorkflowRL.conf_activity('inc', version=1)

worker = SWFWorkflowWorker()
worker.register(QuickReturnWorkflow, QuickReturn)
worker.register(ClosureWorkflow, Closure)
worker.register(ArgumentsWorkflow, Arguments)
worker.register(DependencyWorkflow, Dependency)
worker.register(ParallelWorkflow, Parallel)
worker.register(ParallelWorkflowRL, Parallel)


cases = [{
    'name': 'QuickReturn',
    'version': 1,
    'input_args': [10],
    'expected': {
        'finish': 10,
    },
}, {
    'name': 'QuickReturn',
    'version': 1,
    'input_args': ['abc'],
    'expected': {
        'finish': 'abc',
    },
}, {
    'name': 'QuickReturn',
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
                'call_key': 'inc-0-0',
                'name': 'inc',
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
        'inc-0-0': 1,
        'inc-1-0': 2,
    },
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'inc-2-0',
                'name': 'inc',
                'version': 1,
                'input_args': [2],
            },
        ],
    },
}, {
    'name': 'Parallel',
    'version': 1,
    'input_args': [5],
    'results': {
        'inc-0-0': 1,
        'inc-1-0': 2,
        'inc-2-0': 3,
        'inc-3-0': 4,
        'inc-4-0': 5,
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
                'call_key': 'inc-0-0',
                'name': 'inc',
                'version': 1,
                'input_args': [0],
            },
            {
                'type': 'activity',
                'call_key': 'inc-1-0',
                'name': 'inc',
                'version': 1,
                'input_args': [1],
            },
            {
                'type': 'activity',
                'call_key': 'inc-2-0',
                'name': 'inc',
                'version': 1,
                'input_args': [2],
            },
            {
                'type': 'activity',
                'call_key': 'inc-3-0',
                'name': 'inc',
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
        'inc-0-0',
        'inc-2-0',
        'inc-3-0',
    ],
    'results': {
        'inc-1-0': 2,
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
                'call_key': 'inc-0-0',
                'name': 'inc',
                'version': 1,
                'input_args': [0],
            },
            {
                'type': 'activity',
                'call_key': 'inc-1-0',
                'name': 'inc',
                'version': 1,
                'input_args': [1],
            },
            {
                'type': 'activity',
                'call_key': 'inc-2-0',
                'name': 'inc',
                'version': 1,
                'input_args': [2],
            },
        ],
    },
}]
