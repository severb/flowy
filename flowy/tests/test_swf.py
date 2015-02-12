import json
import pprint
import unittest

from flowy.backend.swf import SWFExecutionHistory

from flowy.tests.swf_cases import worker
from flowy.tests.swf_cases import cases


class DummyDecision(object):
    def __init__(self):
        self.result = None
        self.queued = {
            'schedule': []
        }

    def fail(self, reason):
        if self.result is not None:
            return
        self.result = {'fail': str(reason)}

    def flush(self):
        if self.result is not None:
            return
        self.result = self.queued

    def restart(self, input_data):
        if self.result is not None:
            return
        args, kwargs = json.loads(input_data)
        self.result = {'restart': {'input_args': args, 'input_kwargs': kwargs}}

    def finish(self, result):
        if self.result is not None:
            return
        self.result = {'finish': json.loads(result)}

    default_activity = {
        'input_args': [],
        'input_kwargs': {},
        'task_list': None,
        'heartbeat': None,
        'schedule_to_close': None,
        'schedule_to_start': None,
        'start_to_close': None,
    }

    def schedule_activity(self, call_key, name, version, input_data, task_list,
                          heartbeat, schedule_to_close, schedule_to_start,
                          start_to_close):
        args, kwargs = json.loads(input_data)
        self.queued['schedule'].append({
            'type': 'activity',
            'call_key': call_key,
            'name': name,
            'version': version,
            'input_args': args,
            'input_kwargs': kwargs,
            'task_list': task_list,
            'heartbeat': heartbeat,
            'schedule_to_close': schedule_to_close,
            'schedule_to_start': schedule_to_start,
            'start_to_close': start_to_close,
        })

    default_workflow = {
        'input_args': [],
        'input_kwargs': {},
        'task_list': None,
        'workflow_duration': None,
        'decision_duration': None,
    }

    def schedule_workflow(self, call_key, name, version, input_data, task_list,
                          workflow_duration, decision_duration):
        args, kwargs = json.loads(input_data)
        self.queued['schedule'].append({
            'type': 'workflow',
            'call_key': call_key,
            'name': name,
            'version': version,
            'input_args': args,
            'input_kwargs': kwargs,
            'task_list': task_list,
            'workflow_duration': workflow_duration,
            'decision_duration': decision_duration,
        })

    default_timer = {
        'delay': 0,
    }

    def schedule_timer(self, call_key, delay):
        self.queued['schedule'].append({
            'type': 'timer',
            'call_key': call_key,
            'delay': delay,
        })

    def assert_equals(self, expected):
        if isinstance(expected, dict) and expected.get('schedule') != None:
            schedule = []
            for sched in expected['schedule']:
                if sched['type'] == 'activity':
                    schedule.append(dict(self.default_activity, **sched))
                elif sched['type'] == 'workflow':
                    schedule.append(dict(self.default_workflow, **sched))
                elif sched['type'] == 'timer':
                    schedule.append(dict(self.default_timer, **sched))
                else:
                    assert False, 'Invalid schedule type'
            expected = {'schedule': schedule}
        e = pprint.pformat(expected)
        r = pprint.pformat(self.result)
        m = "Expectation:\n%s\ndoesn't match result:\n%s" % (e, r)
        assert expected == self.result, m


g = globals()
for i, case in enumerate(cases):
    test_class_name = 'Test%s' % case['name']

    if test_class_name not in g:

        class T(unittest.TestCase):
            pass

        T.__name__ = test_class_name
        g[test_class_name] = T
        del T

    def make_t(case):
        def t(self):
            key = str(case['name']), str(case['version'])
            input_args = case.get('input_args', [])
            input_kwargs = case.get('input_kwargs', {})
            input_data = json.dumps([input_args, input_kwargs])
            decision = DummyDecision()
            results = case.get('results', {})
            results = dict((k, str(v)) for k, v in results.items())
            order = (
                list(case.get('results', {}).keys()) 
                + list(case.get('errors', {}).keys())
                + list(case.get('timedout', []))
            )
            execution_history = SWFExecutionHistory(
                case.get('running', []),
                case.get('timedout', []),
                results,
                case.get('errors', {}),
                order,
            )
            worker(key, input_data, decision, execution_history)
            decision.assert_equals(case.get('expected'))
        return t

    t = make_t(case)
    t.__name__ = t_name = 'test_%s' % i
    setattr(g[test_class_name], t_name, t)
    del t
