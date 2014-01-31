from flowy.scanner import activity, workflow
from mock import sentinel as s


a1 = activity(s.task_id, s.task_list)(s.task_factory1)
a2 = activity(
    s.task_id, s.task_list,
    heartbeat='6',
    schedule_to_close=42.0,
    schedule_to_start=12,
    start_to_close=30)(s.task_factory2)
w1 = workflow(s.task_id, s.task_list)(s.task_factory3)
w2 = workflow(
    s.task_id, s.task_list,
    workflow_duration='120',
    decision_duration=5.0)(s.task_factory4)
