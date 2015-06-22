class Decision(dict):
    def __init__(self):
        self['type'] = 'schedule'
        self['activities'] = []
        self['workflows'] = []
        self.closed = False

    def fail(self, reason):
        if self.closed:
            return
        self.clear()
        self['type'] = 'fail'
        self['reason'] = reason
        self.closed = True

    def flush(self):
        self.closed = True

    def restart(self, input_data):
        if self.closed:
            return
        self.clear()
        self['type'] = 'restart'
        self['input_data'] = input_data
        self.closed = True

    def finish(self, result):
        if self.closed:
            return
        self.clear()
        self['type'] = 'finish'
        self['result'] = result
        self.closed = True

    def schedule_activity(self, call_key, input_data, f):
        if self.closed or 'activities' not in self:
            return
        self['activities'].append(
            {'id': call_key,
             'input_data': input_data,
             'f': f})

    def schedule_workflow(self, call_key, input_data, f):
        if self.closed or 'workflows' not in self:
            return
        self['workflows'].append(
            {'id': call_key,
             'input_data': input_data,
             'f': f})


class ActivityDecision(object):
    def __init__(self, decision, identity, f):
        self.decision = decision
        self.identity = identity
        self.f = f

    def fail(self, reason):
        self.decision.fail(reason)

    def schedule(self, call_number, retry_number, delay, input_data):
        self.decision.schedule_activity(
            '%s-%s-%s' % (self.identity, call_number, retry_number),
            input_data, self.f)


class WorkflowDecision(object):
    def __init__(self, decision, identity, f):
        self.decision = decision
        self.identity = identity
        self.f = f

    def fail(self, reason):
        self.decision.fail(reason)

    def schedule(self, call_number, retry_number, delay, input_data):
        self.decision.schedule_workflow(
            '%s-%s-%s' % (self.identity, call_number, retry_number),
            input_data, self.f)
