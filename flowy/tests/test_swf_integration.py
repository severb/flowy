from __future__ import print_function

import os
import sys
import threading
import time

import vcr
import vcr.errors
from flowy import restart
from flowy import SWFActivity
from flowy import SWFActivityWorker
from flowy import SWFWorkflow
from flowy import SWFWorkflowStarter
from flowy import SWFWorkflowWorker

VERSION = 1
W_CASSETTE = 'cassettes/w.yml'
A_CASSETTE = 'cassettes/a.yml'
DOMAIN = 'IntegrationTest'
TASKLIST = 'tl'
IDENTITY = 'test'

a_conf = SWFActivity(version=VERSION,
                     default_task_list=TASKLIST,
                     default_schedule_to_start=30,
                     default_schedule_to_close=60,
                     default_start_to_close=15,
                     default_heartbeat=10)


@a_conf
def tactivity(hb, a=None, b=None, sleep=None, heartbeat=False, err=None):
    print('in activity', a, b, sleep, heartbeat, err)
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
class TestWorkflow(object):
    def __init__(self, activity):
        self.activity = activity

    def __call__(self, r=True):
        return self.activity(10, 11)


wworker = SWFWorkflowWorker()
wworker.scan()
aworker = SWFActivityWorker()
aworker.scan()


def test_activity_integration():
    with vcr.use_cassette(A_CASSETTE, record_mode='none') as cass:
        try:
            aworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass
        assert cass.all_played


def test_workflow_integration():
    with vcr.use_cassette(A_CASSETTE, record_mode='none') as cass:
        try:
            aworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass
        assert cass.all_played


def start_activity_worker():
    with vcr.use_cassette(A_CASSETTE,
                          record_mode='all',
                          filter_headers=['authorization', 'x-amz-date'],
                          match_on=['method', 'uri', 'host', 'port', 'path',
                                    'query', 'body', 'headers']) as cass:
        try:
            aworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass


def start_workflow_worker():
    with vcr.use_cassette(W_CASSETTE,
                          record_mode='all',
                          filter_headers=['authorization', 'x-amz-date'],
                          match_on=['method', 'uri', 'host', 'port', 'path',
                                    'query', 'body', 'headers']) as cass:
        try:
            wworker.run_forever(DOMAIN, TASKLIST, identity=IDENTITY)
        except vcr.errors.CannotOverwriteExistingCassetteException:
            pass
        assert cass.all_played


if __name__ == '__main__':
    try:
        os.remove(A_CASSETTE)
    except:
        pass
    try:
        os.remove(W_CASSETTE)
    except:
        pass

    a_worker_thread = threading.Thread(target=start_activity_worker)
    w_worker_thread = threading.Thread(target=start_workflow_worker)

    a_worker_thread.start()
    w_worker_thread.start()

    time.sleep(5)  # Wait for registration

    starter = SWFWorkflowStarter(DOMAIN, 'TestWorkflow', VERSION)
    starter()

    a_worker_thread.join()
    w_worker_thread.join()
