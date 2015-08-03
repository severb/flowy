import json
import pprint
import unittest

from flowy.swf.history import SWFExecutionHistory
from flowy.proxy import Proxy
from flowy.config import ActivityConfig

from swf_cases import worker
from swf_cases import cases


serialize_result = ActivityConfig.serialize_result
deserialize_result = Proxy.deserialize_result
serialize_input = Proxy.serialize_input
deserialize_input = ActivityConfig.deserialize_input



class DummyDecision(object):
    def __init__(self):
        self.result = None
        self.queued = {'schedule': []}

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
        args, kwargs = deserialize_input(input_data)
        self.result = {'restart': {'input_args': args, 'input_kwargs': kwargs}}

    def finish(self, result):
        if self.result is not None:
            return
        self.result = {'finish': deserialize_result(result)}

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
        args, kwargs = deserialize_input(input_data)
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
        'child_policy': None,
    }

    def schedule_workflow(self, call_key, name, version, input_data, task_list,
                          workflow_duration, decision_duration, child_policy):
        args, kwargs = deserialize_input(input_data)
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
            'child_policy': child_policy,
        })

    default_timer = {'delay': 0, }

    def schedule_timer(self, call_key, delay):
        self.queued['schedule'].append(
            {'type': 'timer',
             'call_key': call_key,
             'delay': delay, })

    def assert_equals(self, expected):
        if isinstance(expected, dict) and 'schedule' in expected:
            schedule = []
            for sched in expected['schedule']:
                if sched['type'] == 'activity':
                    s = dict(self.default_activity, **sched)
                elif sched['type'] == 'workflow':
                    s = dict(self.default_workflow, **sched)
                elif sched['type'] == 'timer':
                    s = dict(self.default_timer, **sched)
                else:
                    assert False, 'Invalid schedule type'
                if 'version' in s:
                    s['version'] = str(s['version'])
                if s.get('heartbeat') is not None:
                    s['heartbeat'] = str(s['heartbeat'])
                if s.get('start_to_close') is not None:
                    s['start_to_close'] = str(s['start_to_close'])
                if s.get('schedule_to_close') is not None:
                    s['schedule_to_close'] = str(s['schedule_to_close'])
                if s.get('schedule_to_start') is not None:
                    s['schedule_to_start'] = str(s['schedule_to_start'])
                if s.get('decision_duration') is not None:
                    s['decision_duration'] = str(s['decision_duration'])
                if s.get('workflow_duration') is not None:
                    s['workflow_duration'] = str(s['workflow_duration'])
                schedule.append(s)
            expected = {'schedule': schedule}
        if isinstance(expected, dict) and 'restart' in expected:
            r = expected['restart']
            expected['restart'] = {
                'input_args': r.get('input_args', []),
                'input_kwargs': r.get('input_kwargs', {})
            }
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
            name, version = str(case['name']), str(case['version'])
            input_args = case.get('input_args', [])
            input_kwargs = case.get('input_kwargs', {})
            input_data = serialize_input(*input_args, **input_kwargs)
            decision = DummyDecision()
            results = case.get('results', {})
            results = dict((k, serialize_result(v)) for k, v in results.items())
            order = (list(case.get('results', {}).keys()) + list(case.get(
                'errors', {}).keys()) + list(case.get('timedout', [])))
            execution_history = SWFExecutionHistory(
                case.get('running', []), case.get('timedout', []), results,
                case.get('errors', {}), case.get('order', order), )
            worker(name, version, input_data, decision, execution_history)
            decision.assert_equals(case.get('expected'))

        return t

    t = make_t(case)
    t.__name__ = t_name = 'test_%s' % i
    setattr(g[test_class_name], t_name, t)
    del t


class TestRegistration(unittest.TestCase):
    def test_already_registered(self):
        from flowy import SWFWorkflowConfig, SWFWorkflowWorker
        worker = SWFWorkflowWorker()
        w = SWFWorkflowConfig()
        worker.register(w, lambda: 1, name='T', version=1)
        self.assertRaises(ValueError, lambda: worker.register(w, lambda: 1, name='T', version=1))

    def test_digit_name(self):
        from flowy import SWFWorkflowConfig
        w = SWFWorkflowConfig()
        self.assertRaises(ValueError, lambda: w.conf_proxy_factory('123', None))

    def test_keyword_name(self):
        from flowy import SWFWorkflowConfig
        w = SWFWorkflowConfig()
        self.assertRaises(ValueError, lambda: w.conf_proxy_factory('for', None))

    def test_nonalnum_name(self):
        from flowy import SWFWorkflowConfig
        w = SWFWorkflowConfig()
        self.assertRaises(ValueError, lambda: w.conf_proxy_factory('abc!123', None))

    def test_duplicate_name(self):
        from flowy import SWFWorkflowConfig
        w = SWFWorkflowConfig()
        w.conf_proxy_factory('task', None)
        self.assertRaises(ValueError, lambda: w.conf_proxy_factory('task', None))


class TestScan(unittest.TestCase):
    def test_scan(self):
        from flowy import SWFWorkflowWorker
        import workflows
        worker = SWFWorkflowWorker()
        worker.scan(package=workflows)
        assert ('NoTask', '1') in worker.registry
        assert ('Closure', '1') in worker.registry
        assert ('Named', '1') in worker.registry


class TestParallelReduce(unittest.TestCase):
    def test_empty_iterable(self):
        from flowy import parallel_reduce
        f = lambda x, y: 1
        self.assertRaises(ValueError, lambda: parallel_reduce(f, []))

    def test_one_element_iterable(self):
        from flowy import parallel_reduce
        f = lambda x, y: 1
        x = object()
        parallel_x = parallel_reduce(f, [x])
        # python 2.6 doesn't have assertIs
        assert x is parallel_x.__wrapped__


class TestFinishOrder(unittest.TestCase):
    def test_non_results(self):
        from flowy import finish_order
        x = [1, 2, 3, 4, 5, 'a', 'b', 'c']
        self.assertEquals(x, list(finish_order(x)))

    def test_mixed(self):
        from flowy import finish_order
        from flowy.result import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()
        fo = list(finish_order([1, e, 2, p, 3, r, t]))
        self.assertEquals(fo[:3], [1, 2, 3])
        self.assertEquals(fo[3].__factory__, r.__factory__)
        self.assertEquals(fo[4].__factory__, t.__factory__)
        self.assertEquals(fo[5].__factory__, e.__factory__)
        self.assertEquals(fo[6].__factory__, p.__factory__)

    def test_results(self):
        from flowy import finish_order
        from flowy.result import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()
        fo = list(finish_order([e, p, r, t]))
        self.assertEquals(fo[0].__factory__, r.__factory__)
        self.assertEquals(fo[1].__factory__, t.__factory__)
        self.assertEquals(fo[2].__factory__, e.__factory__)
        self.assertEquals(fo[3].__factory__, p.__factory__)

    def test_first_non_results(self):
        from flowy import first
        x = [1, 2, 3, 4, 5, 'a', 'b', 'c']
        self.assertEquals(1, first(x))

    def test_first_mixed(self):
        from flowy import first
        from flowy.result import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()

    def test_first_results(self):
        from flowy import first
        from flowy.result import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()
        self.assertEquals(first([e, p, r, t]).__factory__, r.__factory__)
