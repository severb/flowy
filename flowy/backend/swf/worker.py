from boto.swf.layer1 import Layer1
from boto.exception import SWFResponseError

from flowy.utils import setup_default_logger
from flowy.utils import logger
from flowy.worker import Worker
from flowy.backend.swf.decision import SWFActivityDecision


__all__ = ['SWFWorkflowWorker', 'SWFActivityWorker']


_IDENTITY_SIZE = 256


class SWFWorker(Worker):
    def __init__(self, *args, **kwargs):
        super(SWFWorker, self).__init__(*args, **kwargs)
        self.reg_remote = []

    def register_remote(self, layer1, domain):
        """Register or check compatibility of all configs in Amazon SWF."""
        for config, task_factory in self.reg_remote:
            # Raises if there are registration problems
            config.register_remote(layer1, domain, task_factory)

    def register(self, config, task_factory):
        super(SWFWorker, self).register(config, task_factory)
        self.reg_remote.append((config, task_factory))


class SWFWorkflowWorker(SWFWorker):
    categories = ['swf_workflow']

    # Be explicit about what arguments are expected
    def __call__(self, key, input_data, decision, execution_history):
        super(SWFWorkflowWorker, self).__call__(
            key, input_data, decision,    # needed for worker logic
            decision, execution_history)  # extra_args passed to proxies

    def break_loop(self):
        """Used to exit the loop in tests. Return True to break."""
        return False

    def run_forever(self, domain, task_list,
                    layer1=None,
                    setup_log=True,
                    register_remote=True,
                    identity=None):
        """Start an endless single threaded/single process worker loop.

        The worker polls endlessly for new decisions from the specified domain
        and task list and runs them.

        If reg_remote is set, all registered workflow are registered remotely.

        An identity can be set to track this worker in the SWF console,
        otherwise a default identity is generated from this machine domain and
        process pid.

        If setup_log is set, a default configuration for the logger is loaded.

        A custom SWF client can be passed in layer1, otherwise a default client
        is used.

        """
        if setup_log:
            setup_default_logger()
        identity = identity if identity is not None else default_identity()
        identity = str(identity)[:_IDENTITY_SIZE]
        layer1 = layer1 if layer1 is not None else Layer1()
        if register_remote:
            self.register_remote(layer1, domain)
        try:
            while 1:
                if self.break_loop():
                    break
                key, input_data, exec_history, decision = poll_next_decision(
                    layer1, domain, task_list, identity)
                self(key, input_data, decision, exec_history)
        except KeyboardInterrupt:
            pass


class SWFActivityWorker(SWFWorker):
    categories = ['swf_activity']

    #Be explicit about what arguments are expected
    def __call__(self, name, version, input_data, decision):
        # No extra arguments are used
        super(SWFActivityWorker, self).__call__(
            key, input_data, decision,    # needed for worker logic
            decision.heartbeat)           # extra_args

    def break_loop(self):
        """Used to exit the loop in tests. Return True to break."""
        return False

    def run_forever(self, domain, task_list,
                    layer1=None,
                    setup_log=True,
                    register_remote=True,
                    identity=None):
        """Same as SWFWorkflowWorker.run_forever but for activities."""
        if setup_log:
            setup_default_logger()
        identity = identity if identity is not None else default_identity()
        identity = str(identity)[:_IDENTITY_SIZE]
        layer1 = layer1 if layer1 is not None else Layer1()
        if register_remote:
            self.register_remote(layer1, domain)
        try:
            while 1:
                if self.break_loop():
                    break
                swf_response = {}
                while ('taskToken' not in swf_response or
                       not swf_response['taskToken']):
                    try:
                        swf_response = layer1.poll_for_activity_task(
                            domain=domain,
                            task_list=task_list,
                            identity=identity)
                    except SWFResponseError:
                        # add a delay before retrying?
                        logger.exception('Error while polling for activities:')

                at = swf_response['activityType']
                key = (at['name'], at['version'])
                input_data = swf_response['input']
                token = swf_response['taskToken']
                decision = SWFActivityDecision(layer1, token)
                self((at['name'], at['version']), input_data, decision)
        except KeyboardInterrupt:
            pass


def default_identity():
    """Generate a local identity for this process."""
    identity = "%s-%s" % (socket.getfqdn(), os.getpid())
    return identity[-_IDENTITY_SIZE:]  # keep the most important part
