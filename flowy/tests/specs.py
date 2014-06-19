from flowy.scanner import swf_activity as activity, swf_workflow as workflow
from mock import sentinel as s


a1 = activity(s.version, s.task_list)(s.task_factory1)
a2 = activity(s.version, s.task_list,
              heartbeat='6',
              schedule_to_close=42.0,
              schedule_to_start=12,
              start_to_close=30,
              name='activity2')(s.task_factory2)
w1 = workflow(s.version, s.task_list)(s.task_factory3)
w2 = workflow(s.version, s.task_list,
              workflow_duration='120',
              decision_duration=5.0)(s.task_factory4)
