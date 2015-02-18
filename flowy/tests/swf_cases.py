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
SAWorkflowCustomTimersW = SWFWorkflow(name='SACustomTimersW', version=1)
SAWorkflowCustomTimersW.conf_workflow(
    'task', version=1, decision_duration=10, workflow_duration=11,
    task_list='TL', child_policy='TERMINATE')
WaitActivityWorkflow = SWFWorkflow(version=1)
WaitActivityWorkflow.conf_activity('task', version=1)
RestartWorkflow = SWFWorkflow(version=1)
RestartWorkflow.conf_activity('task', version=1)
PreRunWorkflow = SWFWorkflow(version=1)
PreRunWorkflow.conf_activity('task', version=1)
PreRunErrorWorkflow = SWFWorkflow(version=1)
PreRunWaitWorkflow = SWFWorkflow(version=1)
PreRunWaitWorkflow.conf_activity('task', version=1)
DoubleDepWorkflow = SWFWorkflow(version=1)
DoubleDepWorkflow.conf_activity('task', version=1)
FirstWorkflow = SWFWorkflow(version=1)
FirstWorkflow.conf_activity('task', version=1)
First2Workflow = SWFWorkflow(version=1)
First2Workflow.conf_activity('task', version=1)


worker = SWFWorkflowWorker()
worker.register(NoTaskWorkflow, NoTask)
worker.register(ClosureWorkflow, Closure)
worker.register(ArgumentsWorkflow, Arguments)
worker.register(DependencyWorkflow, Dependency)
worker.register(ParallelWorkflow, Parallel)
worker.register(ParallelWorkflowRL, Parallel)
worker.register(UnhandledExceptionWorkflow, UnhandledException)
worker.register(SingleActivityWorkflow, SingleTask)
worker.register(SAWorkflowCustomTimers, SingleTask)
worker.register(SAWorkflowCustomTimersW, SingleTask)
worker.register(WaitActivityWorkflow, WaitTask)
worker.register(RestartWorkflow, Restart)
worker.register(PreRunWorkflow, PreRun)
worker.register(PreRunErrorWorkflow, PreRunError)
worker.register(PreRunWaitWorkflow, PreRunWait)
worker.register(DoubleDepWorkflow, DoubleDep)
worker.register(FirstWorkflow, First)
worker.register(First2Workflow, First2)


cases = [{
    'name': 'NotFound',
    'version': 1,
}, {
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
    'name': 'SingleTask',
    'version': 1,
    'errors': {
        'task-0-0': 'err!',
    },
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'SingleTask',
    'version': 1,
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
            },
        ],
    },
}, {
    'name': 'SingleTask',
    'version': 1,
    'results': {
        'task-0-0': 1,
    },
    'expected': {
        'finish': 1,
    },
}, {
    'name': 'SingleTask',
    'version': 1,
    'timedout': [
        'task-0-0',
    ],
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-1',
                'name': 'task',
                'version': 1,
            },
        ],
    },
}, {
    'name': 'SingleTask',
    'version': 1,
    'timedout': [
        'task-0-0',
        'task-0-1',
        'task-0-2',
    ],
    'expected': {
        'fail': 'A task has timedout',
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
}, {
    'name': 'SACustomTimersW',
    'version': 1,
    'expected': {
        'schedule': [
            {
                'type': 'workflow',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
                'task_list': 'TL',
                'decision_duration': 10,
                'workflow_duration': 11,
                'child_policy': 'TERMINATE',
            },
        ],
    },
}, {
    'name': 'WaitTask',
    'version': 1,
    'running': [
        'task-0-0',
    ],
    'expected': {
        'schedule': [],
    },
}, {
    'name': 'WaitTask',
    'version': 1,
    'results': {
        'task-0-0': 1,
    },
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-1-0',
                'name': 'task',
                'version': 1,
                'input_args': [1],
            },
        ],
    },
}, {
    'name': 'WaitTask',
    'version': 1,
    'errors': {
        'task-0-0': 'err!',
    },
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'WaitTask',
    'version': 1,
    'timedout': [
        'task-0-0',
        'task-0-1',
        'task-0-2',
    ],
    'expected': {
        'fail': 'A task has timedout',
    },
}, {
    'name': 'Restart',
    'version': 1,
    'results': {
        'task-0-0': 1,
    },
    'expected': {
        'restart': {
            'input_args': [1, 2],
        },
    },
}, {
    'name': 'Restart',
    'version': 1,
    'running': [
        'task-0-0',
    ],
    'expected': {
        'schedule': [],
    },
}, {
    'name': 'Restart',
    'version': 1,
    'errors': {
        'task-0-0': 'err!',
    },
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'PreRun',
    'version': 1,
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-0-0',
                'name': 'task',
                'version': 1,
            },
        ],
    },
}, {
    'name': 'PreRun',
    'version': 1,
    'results': {
        'task-0-0': 1,
    },
    'expected': {
        'schedule': [
            {
                'type': 'activity',
                'call_key': 'task-1-0',
                'name': 'task',
                'version': 1,
                'input_args': [1],
            },
        ],
    },
}, {
    'name': 'PreRun',
    'version': 1,
    'errors': {
        'task-0-0': 'err!',
    },
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'PreRunError',
    'version': 1,
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'PreRunWait',
    'version': 1,
    'running': [
        'task-0-0',
    ],
    'expected': {
        'schedule': [],
    },
}, {
    'name': 'DoubleDep',
    'version': 1,
    'running': [
        'task-0-0',
    ],
    'results': {
        'task-1-0': 1,
    },
    'expected': {
        'schedule': [],
    },
}, {
    'name': 'DoubleDep',
    'version': 1,
    'running': [
        'task-1-0',
    ],
    'results': {
        'task-0-0': 1,
    },
    'expected': {
        'schedule': [],
    },
}, {
    'name': 'DoubleDep',
    'version': 1,
    'errors': {
        'task-0-0': 'err1!',
        'task-1-0': 'err2!',
    },
    'order': [
        'task-0-0',
        'task-1-0',
    ],
    'expected': {
        'fail': 'err1!',
    },
}, {
    'name': 'DoubleDep',
    'version': 1,
    'errors': {
        'task-0-0': 'err1!',
        'task-1-0': 'err2!',
    },
    'order': [
        'task-1-0',
        'task-0-0',
    ],
    'expected': {
        'fail': 'err2!',
    },
}, {
    'name': 'First',
    'version': 1,
    'results': {
        'task-0-0': 1,
        'task-1-0': 2,
    },
    'order': [
        'task-0-0',
        'task-1-0',
    ],
    'expected': {
        'finish': 1,
    },
}, {
    'name': 'First',
    'version': 1,
    'results': {
        'task-0-0': 1,
        'task-1-0': 2,
    },
    'order': [
        'task-1-0',
        'task-0-0',
    ],
    'expected': {
        'finish': 2,
    },
}, {
    'name': 'First',
    'version': 1,
    'results': {
        'task-0-0': 1,
    },
    'errors': {
        'task-1-0': 'err!',
    },
    'order': [
        'task-1-0',
        'task-0-0',
    ],
    'expected': {
        'fail': 'err!',
    },
}, {
    'name': 'First2',
    'version': 1,
    'results': {
        'task-0-0': 1,
        'task-1-0': 2,
        'task-2-0': 3,
        'task-3-0': 4,
    },
    'order': [
        'task-0-0',
        'task-3-0',
        'task-1-0',
        'task-2-0',
    ],
    'expected': {
        'finish': [1, 4],
    },
}, {
    'name': 'First2',
    'version': 1,
    'results': {
        'task-0-0': 1,
        'task-1-0': 2,
    },
    'errors': {
        'task-2-0': 'err3!',
        'task-3-0': 'err4!',
    },
    'order': [
        'task-0-0',
        'task-3-0',
        'task-1-0',
        'task-2-0',
    ],
    'expected': {
        'fail': 'err4!',
    },
}, {
    'name': 'First2',
    'version': 1,
    'results': {
        'task-0-0': 1,
    },
    'running': [
        'task-1-0',
        'task-2-0',
        'task-3-0',
    ],
    'expected': {
        'schedule': [],
    },
}]
