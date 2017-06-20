"""Dummy activities used in the workflow example.

There's nothing special about these activities; they are just regular Python
functions.

All activities, when called by Flowy, receive a heartbeat callable as the first
argument. When called, this sends a heartbeat to the backend. During testing
this callable can be mocked by an empty function.
"""

import time


def sum(heartbeat, a, b, sleep=0):
    print 'in sum', a, b, sleep
    time.sleep(sleep)
    return a + b


def mul(heartbeat, a, b, sleep=0):
    print 'in mul', a, b, sleep
    time.sleep(sleep)
    return a * b


def err(heartbeat, reason):
    print 'in err', reason
    raise ValueError(reason)


def heartbeat_example(heartbeat, n):
    """Example activity sending heartbeats in predefined points."""
    for _ in range(n):
        # dosomething
        heartbeat()
