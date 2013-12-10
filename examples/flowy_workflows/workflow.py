from flowy import Workflow, ActivityProxy, WorkflowProxy
from flowy import make_config, workflow_config


@workflow_config('InvertModulo', 2, 'invert_list', 600, 60)
class InvertModulo(Workflow):
    """
    Return inverse of a number mod n.

    """
    div = ActivityProxy(
        name='NumberDivider',
        version=2,
        task_list='invert_list',
    )

    def run(self, remote, a=15, n=77):
        for i in range(2, n/2 + 1):
            r = remote.div(i*a, n)
            print(i)
            print(a, n)
            print(r.result())
            if r.result():
                return i
        return False


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    # Start a workflow
    InvertModuloWF = my_config.workflow_starter('InvertModulo', 2)
    print 'Starting: ', InvertModuloWF(a=15, n=29)

    # Start the workflow loop
    my_config.scan()
    my_config.start_workflow_loop(task_list='invert_list')
