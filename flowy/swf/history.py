from flowy.swf.decision import task_key, timer_key


class SWFExecutionHistory(object):
    def __init__(self, running, timedout, results, errors, order):
        self.running = running
        self.timedout = timedout
        self.results = results
        self.errors = errors
        self.order_ = order

    def is_running(self, call_key):
        return str(call_key) in self.running

    def order(self, call_key):
        return self.order_.index(str(call_key))

    def has_result(self, call_key):
        return str(call_key) in self.results

    def result(self, call_key):
        return self.results[str(call_key)]

    def is_error(self, call_key):
        return str(call_key) in self.errors

    def error(self, call_key):
        return self.errors[str(call_key)]

    def is_timeout(self, call_key):
        return str(call_key) in self.timedout

    def is_timer_ready(self, call_key):
        return timer_key(call_key) in self.results

    def is_timer_running(self, call_key):
        return timer_key(call_key) in self.running


class SWFTaskExecutionHistory(object):
    def __init__(self, exec_history, identity):
        self.exec_history = exec_history
        self.identity = identity

    def __getattr__(self, fname):
        """Compute the key and delegate to exec_history."""
        if fname not in ['is_running', 'is_timeout', 'is_error', 'has_result',
                         'result', 'order', 'error']:
            return getattr(super(SWFTaskExecutionHistory, self), fname)

        delegate_to = getattr(self.exec_history, fname)

        def clos(call_number, retry_number):
            return delegate_to(task_key(self.identity, call_number, retry_number))

        setattr(self, fname, clos)  # cache it
        return clos
