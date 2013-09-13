import pickle


class WorkflowContext(object):
    def __init__(self):
        self.event_to_call_id = {}
        self.scheduled = set()
        self.results = {}
        self.timed_out = set()
        self.with_errors = {}
        self.args = []
        self.kwargs = {}
        self.input = None

    def any_activity_running(self):
        return bool(self.scheduled)

    def is_activity_scheduled(self, call_id):
        return call_id in self.scheduled

    def activity_result(self, call_id, default=None):
        return self.results.get(call_id, default)

    def activity_error(self, call_id, default=None):
        return self.with_errors.get(call_id, default)

    def is_activity_timeout(self, call_id):
        return call_id in self.timed_out

    def set_scheduled(self, call_id, event_id):
        self.event_to_call_id[event_id] = call_id
        self.scheduled.add(call_id)

    def set_result(self, event_id, result):
        self.scheduled.remove(self.event_to_call_id[event_id])
        self.results[self.event_to_call_id[event_id]] = result

    def set_timed_out(self, event_id):
        self.scheduled.remove(self.event_to_call_id[event_id])
        self.timed_out.add(self.event_to_call_id[event_id])

    def set_error(self, event_id, error):
        self.with_errors[self.event_to_call_id[event_id]] = error

    def serialize(self):
        return pickle.dumps(self)
