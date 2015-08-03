from flowy import SWFWorkflowConfig
from flowy import SWFWorkflowWorker

from workflows import *

no_activity_workflow = SWFWorkflowConfig()

task_activity_workflow = SWFWorkflowConfig()
task_activity_workflow.conf_activity('task', version=1)

task_activity_workflow_rl = SWFWorkflowConfig(rate_limit=3)
task_activity_workflow_rl.conf_activity('task', version=1)

sa_workflow_custom_timers = SWFWorkflowConfig()
sa_workflow_custom_timers.conf_activity('task',
                                     version=1,
                                     heartbeat=10,
                                     schedule_to_start=11,
                                     schedule_to_close=12,
                                     start_to_close=13,
                                     task_list='TL', )

sa_workflow_custom_timers_W = SWFWorkflowConfig()
sa_workflow_custom_timers_W.conf_workflow('task',
                                      version=1,
                                      decision_duration=10,
                                      workflow_duration=11,
                                      task_list='TL',
                                      child_policy='TERMINATE')
task_red_activities_workflow = SWFWorkflowConfig()
task_red_activities_workflow.conf_activity('task', version=1)
task_red_activities_workflow.conf_activity('red', version=1)

worker = SWFWorkflowWorker()
worker.register(no_activity_workflow, NoTask, version=1)
worker.register(no_activity_workflow, Closure, version=1)
worker.register(no_activity_workflow, Arguments, version=1)
worker.register(task_activity_workflow, Dependency, version=1)
worker.register(task_activity_workflow, Parallel, version=1)
worker.register(task_activity_workflow_rl, Parallel, version=1, name='ParallelRL')
worker.register(no_activity_workflow, UnhandledException, version=1)
worker.register(task_activity_workflow, SingleTask, version=1)
worker.register(sa_workflow_custom_timers, SingleTask, version=1, name='SACustomTimers')
worker.register(sa_workflow_custom_timers_W, SingleTask, version=1, name='SACustomTimersW')
worker.register(task_activity_workflow, WaitTask, version=1)
worker.register(task_activity_workflow, Restart, version=1)
worker.register(task_activity_workflow, PreRun, version=1)
worker.register(no_activity_workflow, PreRunError, version=1)
worker.register(task_activity_workflow, PreRunWait, version=1)
worker.register(task_activity_workflow, DoubleDep, version=1)
worker.register(task_activity_workflow, First, version=1)
worker.register(task_activity_workflow, First2, version=1)
worker.register(task_red_activities_workflow, ParallelReduce, version=1)
worker.register(task_red_activities_workflow, ParallelReduceCombined, version=1)
worker.register(task_activity_workflow, ArgsStructErrors, version=1)
worker.register(task_activity_workflow, ArgsStructErrorsHandled, version=1)


cases = [
    {'name': 'NotFound',
     'version': 1, }, {
         'name': 'NoTask',
         'version': 1,
         'input_args': [10],
         'expected': {'finish': 10, },
     }, {
         'name': 'NoTask',
         'version': 1,
         'input_args': ['abc'],
         'expected': {'finish': 'abc', },
     }, {
         'name': 'NoTask',
         'version': 1,
         'input_args': [[1, 2, 3]],
         'expected': {'finish': [1, 2, 3], },
     }, {
         'name': 'Closure',
         'version': 1,
         'input_args': [1],
         'expected': {'finish': 1, },
     }, {
         'name': 'Arguments',
         'version': 1,
         'input_args': ['a', 'b'],
         'expected': {'finish': ['a', 'b', 1, 2], },
     }, {
         'name': 'Arguments',
         'version': 1,
         'input_args': ['a', 'b', 'c'],
         'expected': {'finish': ['a', 'b', 'c', 2], },
     }, {
         'name': 'Arguments',
         'version': 1,
         'input_args': ['a', 'b'],
         'input_kwargs': {'d': 'd'},
         'expected': {'finish': ['a', 'b', 1, 'd'], },
     }, {
         'name': 'Dependency',
         'version': 1,
         'input_args': [5],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [0],
             }, ],
         },
     }, {
         'name': 'Dependency',
         'version': 1,
         'input_args': [5],
         'results': {'task-0-0': 1,
                     'task-1-0': 2, },
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-2-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [2],
             }, ],
         },
     }, {
         'name': 'Dependency',
         'version': 1,
         'input_args': [5],
         'results': {'task-0-0': 1, },
         'errors': {'task-1-0': 'err!', },
         'expected': {'fail': 'err!', },
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
         'expected': {'finish': [1, 2, 3, 4, 5], },
     }, {
         'name': 'Parallel',
         'version': 1,
         'input_args': [4],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [0],
             }, {
                 'type': 'activity',
                 'call_key': 'task-1-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [1],
             }, {
                 'type': 'activity',
                 'call_key': 'task-2-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [2],
             }, {
                 'type': 'activity',
                 'call_key': 'task-3-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [3],
             }, ],
         },
     }, {
         'name': 'Parallel',
         'version': 1,
         'input_args': [4],
         'running': ['task-0-0',
                     'task-2-0',
                     'task-3-0', ],
         'results': {'task-1-0': 2, },
         'expected': {'schedule': [], },
     }, {
         'name': 'ParallelRL',
         'version': 1,
         'input_args': [4],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [0],
             }, {
                 'type': 'activity',
                 'call_key': 'task-1-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [1],
             }, {
                 'type': 'activity',
                 'call_key': 'task-2-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [2],
             }, ],
         },
     }, {
         'name': 'UnhandledException',
         'version': 1,
         'expected': {'fail': 'err!', },
     }, {
         'name': 'SingleTask',
         'version': 1,
         'errors': {'task-0-0': 'err!', },
         'expected': {'fail': 'err!', },
     }, {
         'name': 'SingleTask',
         'version': 1,
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
             }, ],
         },
     }, {
         'name': 'SingleTask',
         'version': 1,
         'results': {'task-0-0': 1, },
         'expected': {'finish': 1, },
     }, {
         'name': 'SingleTask',
         'version': 1,
         'timedout': ['task-0-0', ],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-1',
                 'name': 'task',
                 'version': 1,
             }, ],
         },
     }, {
         'name': 'SingleTask',
         'version': 1,
         'timedout': ['task-0-0',
                      'task-0-1',
                      'task-0-2', ],
         'expected': {'fail': 'A task has timedout', },
     }, {
         'name': 'SACustomTimers',
         'version': 1,
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
                 'task_list': 'TL',
                 'schedule_to_start': 11,
                 'schedule_to_close': 12,
                 'start_to_close': 13,
                 'heartbeat': 10,
             }, ],
         },
     }, {
         'name': 'SACustomTimersW',
         'version': 1,
         'expected': {
             'schedule': [{
                 'type': 'workflow',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
                 'task_list': 'TL',
                 'decision_duration': 10,
                 'workflow_duration': 11,
                 'child_policy': 'TERMINATE',
             }, ],
         },
     }, {
         'name': 'WaitTask',
         'version': 1,
         'running': ['task-0-0', ],
         'expected': {'schedule': [], },
     }, {
         'name': 'WaitTask',
         'version': 1,
         'results': {'task-0-0': 1, },
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-1-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [1],
             }, ],
         },
     }, {
         'name': 'WaitTask',
         'version': 1,
         'errors': {'task-0-0': 'err!', },
         'expected': {'fail': 'err!', },
     }, {
         'name': 'WaitTask',
         'version': 1,
         'timedout': ['task-0-0',
                      'task-0-1',
                      'task-0-2', ],
         'expected': {'fail': 'A task has timedout', },
     }, {
         'name': 'Restart',
         'version': 1,
         'results': {'task-0-0': 1, },
         'expected': {'restart': {'input_args': [1, 2], }, },
     }, {
         'name': 'Restart',
         'version': 1,
         'running': ['task-0-0', ],
         'expected': {'schedule': [], },
     }, {
         'name': 'Restart',
         'version': 1,
         'errors': {'task-0-0': 'err!', },
         'expected': {'fail': 'err!', },
     }, {
         'name': 'PreRun',
         'version': 1,
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
             }, ],
         },
     }, {
         'name': 'PreRun',
         'version': 1,
         'results': {'task-0-0': 1, },
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-1-0',
                 'name': 'task',
                 'version': 1,
                 'input_args': [1],
             }, ],
         },
     }, {
         'name': 'PreRun',
         'version': 1,
         'errors': {'task-0-0': 'err!', },
         'expected': {'fail': 'err!', },
     },
    {'name': 'PreRunError',
     'version': 1,
     'expected': {'fail': 'err!', }, }, {
         'name': 'PreRunWait',
         'version': 1,
         'running': ['task-0-0', ],
         'expected': {'schedule': [], },
     }, {
         'name': 'DoubleDep',
         'version': 1,
         'running': ['task-0-0', ],
         'results': {'task-1-0': 1, },
         'expected': {'schedule': [], },
     }, {
         'name': 'DoubleDep',
         'version': 1,
         'running': ['task-1-0', ],
         'results': {'task-0-0': 1, },
         'expected': {'schedule': [], },
     }, {
         'name': 'DoubleDep',
         'version': 1,
         'errors': {'task-0-0': 'err1!',
                    'task-1-0': 'err2!', },
         'order': ['task-0-0',
                   'task-1-0', ],
         'expected': {'fail': 'err1!', },
     }, {
         'name': 'DoubleDep',
         'version': 1,
         'errors': {'task-0-0': 'err1!',
                    'task-1-0': 'err2!', },
         'order': ['task-1-0',
                   'task-0-0', ],
         'expected': {'fail': 'err2!', },
     }, {
         'name': 'First',
         'version': 1,
         'results': {'task-0-0': 1,
                     'task-1-0': 2, },
         'order': ['task-0-0',
                   'task-1-0', ],
         'expected': {'finish': 1, },
     }, {
         'name': 'First',
         'version': 1,
         'results': {'task-0-0': 1,
                     'task-1-0': 2, },
         'order': ['task-1-0',
                   'task-0-0', ],
         'expected': {'finish': 2, },
     }, {
         'name': 'First',
         'version': 1,
         'results': {'task-0-0': 1, },
         'errors': {'task-1-0': 'err!', },
         'order': ['task-1-0',
                   'task-0-0', ],
         'expected': {'fail': 'err!', },
     }, {
         'name': 'First2',
         'version': 1,
         'results':
         {'task-0-0': 1,
          'task-1-0': 2,
          'task-2-0': 3,
          'task-3-0': 4, },
         'order': ['task-0-0',
                   'task-3-0',
                   'task-1-0',
                   'task-2-0', ],
         'expected': {'finish': [1, 4], },
     }, {
         'name': 'First2',
         'version': 1,
         'results': {'task-0-0': 1,
                     'task-1-0': 2, },
         'errors': {'task-2-0': 'err3!',
                    'task-3-0': 'err4!', },
         'order': ['task-0-0',
                   'task-3-0',
                   'task-1-0',
                   'task-2-0', ],
         'expected': {'fail': 'err4!', },
     }, {
         'name': 'First2',
         'version': 1,
         'results': {'task-0-0': 1, },
         'running': ['task-1-0',
                     'task-2-0',
                     'task-3-0', ],
         'expected': {'schedule': [], },
     }, {
         'name': 'ParallelReduce',
         'version': 1,
         'running': ['task-0-0',
                     'task-1-0',
                     'task-2-0', ],
         'expected': {'schedule': [], },
     }, {
         'name': 'ParallelReduce',
         'version': 1,
         'results': {'task-0-0': 1, },
         'running': ['task-1-0',
                     'task-2-0', ],
         'expected': {'schedule': [], },
     }, {
         'name': 'ParallelReduce',
         'version': 1,
         'results': {'task-0-0': 1,
                     'task-1-0': 2, },
         'order': ['task-0-0',
                   'task-1-0', ],
         'running': ['task-2-0', ],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'red-0-0',
                 'name': 'red',
                 'version': 1,
                 'input_args': [1, 2],
             }, ],
         },
     }, {
         'name': 'ParallelReduce',
         'version': 1,
         'results': {'task-0-0': 1,
                     'task-1-0': 2, },
         'order': ['task-1-0',
                   'task-0-0', ],
         'running': ['task-2-0', ],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'red-0-0',
                 'name': 'red',
                 'version': 1,
                 'input_args': [2, 1],
             }, ],
         },
     }, {
         'name': 'ParallelReduce',
         'version': 1,
         'results':
         {'task-0-0': 1,
          'task-1-0': 2,
          'red-0-0': 3,
          'task-2-0': 10, },
         'order': ['task-1-0',
                   'task-0-0',
                   'red-0-0',
                   'task-2-0', ],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'red-1-0',
                 'name': 'red',
                 'version': 1,
                 'input_args': [3, 10],
             }, ],
         },
     }, {
         'name': 'ParallelReduce',
         'version': 1,
         'results': {
             'task-0-0': 1,
             'task-1-0': 2,
             'red-0-0': 3,
             'task-2-0': 10,
             'red-1-0': 13,
         },
         'order': ['task-1-0',
                   'task-0-0',
                   'red-0-0',
                   'task-2-0',
                   'red-1-0', ],
         'expected': {'finish': 13, },
     }, {
         'name': 'ParallelReduceCombined',
         'version': 1,
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'task-0-0',
                 'name': 'task',
                 'version': 1,
             },
                          {
                              'type': 'activity',
                              'call_key': 'red-0-0',
                              'name': 'red',
                              'version': 1,
                              'input_args': ['a', 'b'],
                          }, ],
         },
     }, {
         'name': 'ParallelReduceCombined',
         'version': 1,
         'results': {'red-0-0': 'ab', },
         'running': ['task-0-0', ],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'red-1-0',
                 'name': 'red',
                 'version': 1,
                 'input_args': ['c', 'ab'],
             }, ],
         },
     }, {
         'name': 'ParallelReduceCombined',
         'version': 1,
         'results': {'task-0-0': 'xyz', },
         'running': ['red-0-0', ],
         'expected': {
             'schedule': [{
                 'type': 'activity',
                 'call_key': 'red-1-0',
                 'name': 'red',
                 'version': 1,
                 'input_args': ['c', 'xyz'],
             }, ],
         },
     }, {
         'name': 'ArgsStructErrors',
         'version': 1,
         'errors': {'task-0-0': 'Err1!', },
         'expected': {'fail': 'Err1!', },
         'running': ['task-1-0', ],
     }, {
         'name': 'ArgsStructErrors',
         'version': 1,
         'errors': {
             'task-0-0': 'Err1!',
             'task-1-0': 'Err2!',
          },
         'order': ['task-1-0', 'task-0-0'],
         'expected': {'fail': 'Err2!', },
     }, {
         'name': 'ArgsStructErrors',
         'version': 1,
         'errors': {
             'task-0-0': 'Err1!',
             'task-1-0': 'Err2!',
          },
         'order': ['task-0-0', 'task-1-0'],
         'expected': {'fail': 'Err1!', },
     }, {
         'name': 'ArgsStructErrorsHandled',
         'version': 1,
         'errors': {
             'task-0-0': 'Err1!',
             'task-1-0': 'Err2!',
          },
         'order': ['task-0-0', 'task-1-0'],
         'expected': {'finish': 8},
     }
]
