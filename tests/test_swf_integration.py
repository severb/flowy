from __future__ import print_function

import os
import sys
import multiprocessing
import time
import json
import uuid
import random
import functools
import gzip

import vcr
import vcr.cassette
import vcr.errors
import vcr.serialize
import vcr.request
from boto.swf.layer1 import Layer1
from flowy import restart
from flowy import wait
from flowy import TaskError
from flowy import SWFActivityConfig
from flowy import SWFActivityWorker
from flowy import SWFWorkflowConfig
from flowy import SWFWorkflowStarter
from flowy import SWFWorkflowWorker

VERSION = 2
HERE = os.path.dirname(os.path.realpath(__file__))
A_CASSETTE = os.path.join(HERE, 'cassettes/a.yml.gz')
W_CASSETTE = os.path.join(HERE, 'cassettes/w.yml.gz')
DOMAIN = 'IntegrationTest'
TASKLIST = 'tl'
IDENTITY = 'test'

RECORDING = False

exit_event = multiprocessing.Event()
wf_finished_event = multiprocessing.Event()


# Patch vcr to use gzip files

def load_cassette(cassette_path, serializer):
    f = gzip.open(cassette_path, 'rb')
    cassette_content = f.read()
    cassette = vcr.serialize.deserialize(cassette_content, serializer)
    f.close()
    return cassette


def save_cassette(cassette_path, cassette_dict, serializer):
    data = vcr.serialize.serialize(cassette_dict, serializer)
    dirname, _ = os.path.split(cassette_path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname)
    f = gzip.open(cassette_path, 'wb')
    f.write(data)
    f.close()


vcr.cassette.load_cassette = load_cassette
vcr.cassette.save_cassette = save_cassette

# Patch requests_match in cassette for speed-up

def requests_match(r1, r2, matchers):
    """Skip logging and speed-up maching."""
    return all(m(r1, r2) for m in matchers)

vcr.cassette.requests_match = requests_match

# Patch urpalse to speed-up for python3
try:
    from functools import lru_cache
    from urllib.parse import urlparse
    vcr.request.urlparse = lru_cache(maxsize=None)(urlparse)
except ImportError:
    pass


# patch uuid4 for consistent keys
def fake_uuid4():
    x = 0
    while 1:
        yield 'fakeuuid-%s-' % x
        x += 1

uuid.uuid4 = functools.partial(next, fake_uuid4())


def break_loop(self):
    return exit_event.is_set()


class TestSWFWorkflowWorker(SWFWorkflowWorker):
    break_loop = break_loop


class TestSWFActivityWorker(SWFActivityWorker):
    break_loop = break_loop


class BaseWorkflow(object):
    def __call__(self, *args, **kwargs):
        r = self.call(*args, **kwargs)
        wait(r)
        wf_finished_event.set()
        return r

    def call(self, *args, **kwargs):
        raise NotImplementedError


a_conf = SWFActivityConfig(default_task_list=TASKLIST,
                           default_schedule_to_start=30,
                           default_schedule_to_close=60,
                           default_start_to_close=15,
                           default_heartbeat=10)


@a_conf(version=VERSION)
def tactivity(hb, a=None, b=None, sleep=None, heartbeat=False, err=None):
    result = None
    if a is not None and b is not None:
        result = a + b
    elif a is not None:
        result = a * a
    if sleep is not None and RECORDING:
        time.sleep(sleep)
    if heartbeat:
        hb()
    if err is not None:
        raise RuntimeError(err)
    return result


empty_conf = SWFWorkflowConfig(default_task_list=TASKLIST,
                               default_decision_duration=10,
                               default_workflow_duration=20,
                               default_child_policy='TERMINATE', )
empty_conf.conf_activity('activity', VERSION, 'tactivity')


@empty_conf(version=VERSION)
class TWorkflow(object):
    def __init__(self, activity):
        pass

    def __call__(self, a=None, b=None, sleep=None, heartbeat=False, err=None):
        dummy_heartbeat = lambda: True
        return tactivity(dummy_heartbeat, a, b, sleep, heartbeat, err)


conf_use_activities = SWFWorkflowConfig(default_task_list=TASKLIST,
                                        default_decision_duration=10,
                                        default_workflow_duration=60,
                                        default_child_policy='TERMINATE')
conf_use_activities.conf_activity('task', VERSION, 'tactivity')
conf_use_activities.conf_activity('short_task', VERSION, 'tactivity',
                                  schedule_to_close=1,
                                  retry=(0, ))
conf_use_activities.conf_activity('delayed_task', VERSION, 'tactivity',
                                  retry=(3, ))
conf_use_activities.conf_activity('non_existing_task', 1, 'xxx')

conf_use_workflow = SWFWorkflowConfig(default_task_list=TASKLIST,
                                      default_decision_duration=10,
                                      default_workflow_duration=60,
                                      default_child_policy='TERMINATE')
conf_use_workflow.conf_workflow('task', VERSION, 'TWorkflow')
conf_use_workflow.conf_workflow('short_task', VERSION, 'TWorkflow',
                                workflow_duration=1,
                                retry=(0, ))
conf_use_workflow.conf_workflow('delayed_task', VERSION, 'TWorkflow',
                                retry=(3, ))
conf_use_workflow.conf_workflow('non_existing_task', 1, 'xxx')


@conf_use_activities(version=VERSION)
@conf_use_workflow(version=VERSION, name='TestWorkflowW')
class TestWorkflow(BaseWorkflow):
    def __init__(self, task, short_task, delayed_task, non_existing_task):
        self.task = task
        self.short_task = short_task
        self.delayed_task = delayed_task
        self.non_existing_task = non_existing_task

    def call(self):
        tasks = [self.task(10),
                 self.task(err=u'Error!'),
                 self.task(heartbeat=True),
                 self.short_task(sleep=3),
                 self.delayed_task(20),
                 self.non_existing_task(), ]
        last = self.task(1, 1)  # Make the history longer, to have pages
        for _ in range(20):
            last = self.task(last, 1)
        tasks.append(last)
        for t in tasks:
            try:
                wait(t)
            except TaskError:
                pass


@empty_conf(version=VERSION)
class RestartWorkflow(BaseWorkflow):
    def __init__(self, activity):
        pass

    def call(self, should_restart=True):
        if should_restart:
            return restart(should_restart=False)
        return 1


@empty_conf(version=VERSION)
class ExitWorkflow(object):
    def __init__(self, activity):
        exit_event.set()
        wait(activity())  # wake the activity thread

    def __call__(self):
        pass


wworker = TestSWFWorkflowWorker()
wworker.scan(package=sys.modules[__name__])
aworker = TestSWFActivityWorker()
aworker.scan(package=sys.modules[__name__])


body_cache = {}
def body_as_dict(r1, r2):
    if r1 not in body_cache:
        r1b = r1.body if isinstance(r1.body, str) else r1.body.decode('utf-8')
        body_cache[r1] = json.loads(r1b)
    if r2 not in body_cache:
        r2b = r2.body if isinstance(r2.body, str) else r2.body.decode('utf-8')
        body_cache[r2] = json.loads(r2b)
    return body_cache[r1] == body_cache[r2]


def escaped_headers(r1, r2):
    import urllib
    r1h = dict((h, urllib.unquote(v)) for h, v in r1.headers.items())
    r2h = dict((h, urllib.unquote(v)) for h, v in r1.headers.items())
    return r1 == r2


vcr.default_vcr.register_matcher('dict_body', body_as_dict)
vcr.default_vcr.register_matcher('esc_headers', body_as_dict)

cassette_args = {
    'match_on': ['dict_body', 'esc_headers', 'query', 'method', 'uri', 'host',
                 'port', 'path'],
    'filter_headers': ['authorization', 'x-amz-date', 'content-length',
                       'user-agent']
}


def test_activity_integration():
    with vcr.use_cassette(A_CASSETTE,
                          record_mode='none', **cassette_args) as cass:
        try:
            l1 = Layer1(aws_access_key_id='x', aws_secret_access_key='x')
            aworker.run_forever(DOMAIN, TASKLIST,
                                identity=IDENTITY,
                                layer1=l1,
                                setup_log=False)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass
        assert cass.all_played


def test_workflow_integration():
    with vcr.use_cassette(W_CASSETTE,
                          record_mode='none', **cassette_args) as cass:
        try:
            l1 = Layer1(aws_access_key_id='x', aws_secret_access_key='x')
            wworker.run_forever(DOMAIN, TASKLIST,
                                identity=IDENTITY,
                                layer1=l1,
                                setup_log=False)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass
        assert cass.all_played


def start_activity_worker():
    with vcr.use_cassette(A_CASSETTE,
                          record_mode='all', **cassette_args) as cass:
        try:
            aworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass


def start_workflow_worker():
    with vcr.use_cassette(W_CASSETTE,
                          record_mode='all', **cassette_args) as cass:
        try:
            wworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass


if __name__ == '__main__':
    RECORDING = True
    try:
        os.remove(A_CASSETTE)
    except:
        pass
    try:
        os.remove(W_CASSETTE)
    except:
        pass

    a_worker = multiprocessing.Process(target=start_activity_worker)
    w_worker = multiprocessing.Process(target=start_workflow_worker)

    a_worker.start()
    w_worker.start()

    time.sleep(5)  # Wait for registration

    wfs = ['TestWorkflow', 'TestWorkflowW', 'RestartWorkflow']
    for wf in wfs:
        print('Starting', wf)
        SWFWorkflowStarter(DOMAIN, wf, VERSION)()
        wf_finished_event.wait()
        wf_finished_event.clear()

    # Must be the last one
    print('Prepare to exit')
    SWFWorkflowStarter(DOMAIN, 'ExitWorkflow', VERSION)()

    a_worker.join()
    w_worker.join()
