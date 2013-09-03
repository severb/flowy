class WorkflowContext(object):
    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def id(self):
        return (
            self.history['workflowType']['name'],
            self.history['workflowType']['version']
        )

    @property
    def args(self):
        return []

    @property
    def kwargs(self):
        return {}

    def execute(self, runner):
        runner_instance = runner(WorkflowState(self.api_response))
        runner_instance(*self.args, **self.kwargs)


class WorkflowState(object):
    def __init__(self, api_response):
        self.running = []
        self.results = {}

    def is_running(self, invocation_id):
        pass

    def result_for(self, invocation_id, default=None):
        pass


class ActivityContext(object):
    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def id(self):
        pass

    def execute(self, runner):
        runner(*self.args, **self.kwargs)
