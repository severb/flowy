from pyswf.workflow import Workflow, Activity
from pyswf.client import SWFClient


class TestWF(Workflow):
    name = 'HelloWorldWorkflow.hello_workflow'
    version = '1'

    rubyf = Activity(
        'HelloWorldActivity.hello_activity', 'my_first_activity'
    )

    def run(self):
        x = self.rubyf()
        y = self.rubyf()
        z = x.result() + y.result()
        print z


c = SWFClient(workflows=[TestWF])
c.run('SeversTest', 'hello_world_task_list_ruby')
