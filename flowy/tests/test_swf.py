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
        'child_policy': None,
    }

    def schedule_workflow(self, call_key, name, version, input_data, task_list,
                          workflow_duration, decision_duration, child_policy):
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
            'child_policy': child_policy,
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
        if isinstance(expected, dict) and 'schedule' in expected:
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
            key = str(case['name']), str(case['version'])
            input_args = case.get('input_args', [])
            input_kwargs = case.get('input_kwargs', {})
            input_data = json.dumps([input_args, input_kwargs])
            decision = DummyDecision()
            results = case.get('results', {})
            results = dict((k, json.dumps(v)) for k, v in results.items())
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
                case.get('order', order),
            )
            worker(key, input_data, decision, execution_history)
            decision.assert_equals(case.get('expected'))
        return t

    t = make_t(case)
    t.__name__ = t_name = 'test_%s' % i
    setattr(g[test_class_name], t_name, t)
    del t


class TestRegistration(unittest.TestCase):
    def test_already_registered(self):
        from flowy import SWFWorkflow, SWFWorkflowWorker
        worker = SWFWorkflowWorker()
        w = SWFWorkflow(name='T', version=1)
        worker.register(w, lambda: 1)
        self.assertRaises(
            ValueError, lambda: worker.register(w, lambda: 1))

    def test_digit_name(self):
        from flowy import SWFWorkflow
        w = SWFWorkflow(name='T', version=1)
        self.assertRaises(
            ValueError, lambda: w.conf_proxy('123', None))

    def test_keyword_name(self):
        from flowy import SWFWorkflow
        w = SWFWorkflow(name='T', version=1)
        self.assertRaises(
            ValueError, lambda: w.conf_proxy('for', None))

    def test_nonalnum_name(self):
        from flowy import SWFWorkflow
        w = SWFWorkflow(name='T', version=1)
        self.assertRaises(
            ValueError, lambda: w.conf_proxy('abc!123', None))

    def test_duplicate_name(self):
        from flowy import SWFWorkflow
        w = SWFWorkflow(name='T', version=1)
        w.conf_proxy('task', None)
        self.assertRaises(
            ValueError, lambda: w.conf_proxy('task', None))


class TestScan(unittest.TestCase):
    def test_scan(self):
        from flowy import SWFWorkflowWorker
        worker = SWFWorkflowWorker()
        worker.scan()
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
        self.assertEquals(x, finish_order(x))

    def test_mixed(self):
        from flowy import finish_order
        from flowy.base import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()
        fo = finish_order([1, e, 2, p, 3, r, t])
        self.assertEquals(fo[:3], [1, 2, 3])
        self.assertEquals(fo[3].__factory__, r.__factory__)
        self.assertEquals(fo[4].__factory__, t.__factory__)
        self.assertEquals(fo[5].__factory__, e.__factory__)
        self.assertEquals(fo[6].__factory__, p.__factory__)

    def test_results(self):
        from flowy import finish_order
        from flowy.base import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()
        fo = finish_order([e, p, r, t])
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
        from flowy.base import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()

    def test_first_results(self):
        from flowy import first
        from flowy.base import result, error, timeout, placeholder
        r = result(1, 1)
        t = timeout(2)
        e = error('err!', 3)
        p = placeholder()
        self.assertEquals(first([e, p, r, t]).__factory__, r.__factory__)


class TestResultJsonTransport(unittest.TestCase):
    def _get_uut(self):
        from flowy.backend.swf import _serialize_result, _deserialize_result
        from flowy.base import result
        return result, _serialize_result, _deserialize_result

    def test_int(self):
        r, s, ds = self._get_uut()
        self.assertEquals(ds(s(r(10, 0))), 10)

    def test_str(self):
        r, s, ds = self._get_uut()
        self.assertEquals(ds(s(r('abc', 0))), 'abc')

    def test_list(self):
        r, s, ds = self._get_uut()
        self.assertEquals(ds(s(r(['abc', 2, 3], 0))), ['abc', 2, 3])

    def test_dict(self):
        r, s, ds = self._get_uut()
        self.assertEquals(ds(s(r({'a': 1, 'b': 2}, 0))), {'a': 1, 'b': 2})

    def test_combined(self):
        r, s, ds = self._get_uut()
        self.assertEquals(ds(s(r([r(1, 0)], 0))), [1])
