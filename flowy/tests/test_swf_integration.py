from __future__ import print_function

import os
import sys
import multiprocessing
import time

import vcr
import vcr.errors
from flowy import restart
from flowy import wait
from flowy import SWFActivity
from flowy import SWFActivityWorker
from flowy import SWFWorkflow
from flowy import SWFWorkflowStarter
from flowy import SWFWorkflowWorker

VERSION = 1
HERE = os.path.dirname(os.path.realpath(__file__))
A_CASSETTE = os.path.join(HERE, 'cassettes/a.yml')
W_CASSETTE = os.path.join(HERE, 'cassettes/w.yml')
DOMAIN = 'IntegrationTest'
TASKLIST = 'tl'
IDENTITY = 'test'

exit_event = multiprocessing.Event()
wf_finished_event = multiprocessing.Event()


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


a_conf = SWFActivity(version=VERSION,
                     default_task_list=TASKLIST,
                     default_schedule_to_start=30,
                     default_schedule_to_close=60,
                     default_start_to_close=15,
                     default_heartbeat=10)


@a_conf
def tactivity(hb, a=None, b=None, sleep=None, heartbeat=False, err=None):
    result = None
    if a is not None and b is not None:
        result = a + b
    elif a is not None:
        result = a * a
    if sleep is not None:
        time.sleep(sleep)
    if heartbeat:
        hb()
    if err is not None:
        raise RuntimeError(err)
    return result


w_conf = SWFWorkflow(version=VERSION,
                     default_task_list=TASKLIST,
                     default_decision_duration=10,
                     default_workflow_duration=20,
                     default_child_policy='TERMINATE')
w_conf.conf_activity('activity', VERSION, 'tactivity')


@w_conf
class TestWorkflow(BaseWorkflow):
    def __init__(self, activity):
        self.activity = activity

    def call(self):
        return self.activity(10, 11)


@w_conf
class ExitWorkflow(object):
    def __init__(self, activity):
        exit_event.set()
        wait(activity())  # notify the activity thread

    def __call__(self):
        pass


wworker = TestSWFWorkflowWorker()
wworker.scan(package=sys.modules[__name__])
aworker = TestSWFActivityWorker()
aworker.scan(package=sys.modules[__name__])

cassette_args = {
    'match_on': ['method', 'uri', 'host', 'port', 'path', 'query', 'body',
                 'headers'],
    'filter_headers': ['authorization', 'x-amz-date', 'content-length',
                       'user-agent']
}


def test_activity_integration():
    with vcr.use_cassette(A_CASSETTE,
                          record_mode='none', **cassette_args) as cass:
        try:
            aworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass
        assert cass.all_played


def test_workflow_integration():
    with vcr.use_cassette(W_CASSETTE,
                          record_mode='none', **cassette_args) as cass:
        try:
            wworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
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

    wfs = ['TestWorkflow']
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
