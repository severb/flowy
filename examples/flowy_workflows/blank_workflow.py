from flowy.swf.boilerplate import start_workflow_worker
from flowy.swf.scanner import workflow
from flowy.task import Workflow
from flowy.swf.starter import WorkflowStarter


@workflow('BlankWorkflow', 3, 'a_list', decision_duration=60,
          workflow_duration=60)
class BlankWorkflow(Workflow):
    """
    Does nothing

    """

    def run(self):
        return True


if __name__ == '__main__':
    f = open("/home/local/3PILLAR/rszabo/flowy/mocks_output.txt", "w")
    f.close()
    f = open("/home/local/3PILLAR/rszabo/flowy/mocks.txt", "w")
    f.close()

    # Start a workflow
    BlankWorkflowId = WorkflowStarter('RolisTest', 'BlankWorkflow', 3,
                                      task_list='b_list')
    BlankWorkflowId()
    print('started')

    start_workflow_worker('RolisTest', task_list='b_list', loop=5)
