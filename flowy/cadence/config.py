import functools

from botocore.exceptions import ClientError

from flowy.cadence.proxy import SWFActivityProxyFactory
from flowy.cadence.proxy import SWFWorkflowProxyFactory
from flowy.config import ActivityConfig
from flowy.config import WorkflowConfig
from flowy.utils import DescCounter
from flowy.utils import logger
from flowy.utils import str_or_none


class SWFConfigMixin(object):
    def register_remote(self, swf_client, domain, name, version):
        """Register the config in Amazon SWF if it's missing.

        If the task registration fails because there is another task registered
        with the same name and version, check if all the defaults have the same
        values.

        If the registration is unsuccessful and the registered version is
        incompatible with this one or in case of SWF communication errors raise
        SWFRegistrationError. ValueError is raised if any configuration values
        can't be converted to the required types.
        """
        registered_as_new = self.try_register_remote(swf_client, domain, name, version)
        if not registered_as_new:
            self.check_compatible(swf_client, domain, name, version)    # raises if incompatible

    def register(self, registry, key, func):
        name, version = key
        if name is None:
            name = func.__name__
        name, version = str(name), str(version)
        registry.register_task((name, version), self.wrap(func))
        registry.add_remote_reg_callback(
            functools.partial(self.register_remote, name=name, version=version))

    def __call__(self, version, name=None):
        key = (name, version)
        return super(SWFConfigMixin, self).__call__(key)


class SWFActivityConfig(SWFConfigMixin, ActivityConfig):
    """A configuration object for Amazon SWF Activities."""
    category = 'swf_activity'  # venusian category used for this type of confs

    def __init__(self, deserialize_input=None, serialize_result=None):
        """Initialize the config object.

        The timer values are in seconds.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies pointing to this activity.

        The name is optional. If no name is set, it will default to the
        function name.
        """
        super(SWFActivityConfig, self).__init__(deserialize_input, serialize_result)

    def try_register_remote(self, swf_client, domain, name, version):
        """Register the activity remotely.

        Returns True if registration is successful and False if another
        activity with the same name is already registered.

        Raise SWFRegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.

        :type swf_client: :class:`flowy.swf.client.SWFClient`
        :param swf_client: an implementation or duck typing of `SWFClient`
        :param domain: the domain name where to register the activity
        :param name: name of the activity
        :param version: version of the activity
        :rtype: bool
        """
        return False

    def check_compatible(self, swf_client, domain, name, version):
        """Check if the remote config has the same defaults as this one.

        Raise SWFRegistrationError in case of SWF communication errors or
        incompatibility and ValueError if any configuration values can't be
        converted to the required types.

        :type swf_client: :class:`flowy.swf.client.SWFClient`
        :param swf_client: an implementation or duck typing of `SWFClient`
        :param domain: the domain name where to register the activity
        :param name: name of the activity
        :param version: version of the activity
        """
        return True


class SWFWorkflowConfig(SWFConfigMixin, WorkflowConfig):
    """A configuration object suited for Amazon SWF Workflows.

    Use conf_activity and conf_workflow to configure workflow implementation
    dependencies.
    """

    category = 'swf_workflow'  # venusian category used for this type of confs

    def __init__(self,
                 default_task_list=None,
                 default_workflow_duration=None,
                 default_decision_duration=None,
                 default_child_policy=None,
                 rate_limit=64,
                 deserialize_input=None,
                 serialize_result=None,
                 serialize_restart_input=None):
        """Initialize the config object.

        The timer values are in seconds. The child policy should be one fo
        TERMINATE, REQUEST_CANCEL, ABANDON or None.

        For the default configs, a value of None means that the config is unset
        and must be set explicitly in proxies pointing to this activity.

        The rate_limit is used to limit the number of concurrent tasks. A value
        of None means no rate limit.
        """
        super(SWFWorkflowConfig, self).__init__(
            deserialize_input, serialize_result, serialize_restart_input)
        self.rate_limit = rate_limit

    def try_register_remote(self, swf_client, domain, name, version):
        """Register the workflow remotely.

        Returns True if registration is successful and False if another
        workflow with the same name is already registered.

        A name should be set before calling this method or RuntimeError is
        raised.

        Raise SWFRegistrationError in case of SWF communication errors and
        ValueError if any configuration values can't be converted to the
        required types.

        :type swf_client: :class:`flowy.swf.client.SWFClient`
        :param swf_client: an implementation or duck typing of `SWFClient`
        :param domain: the domain name where to register the workflow
        :param name: name of the workflow
        :param version: version of the workflow
        :rtype: bool
        """
        return False

    def check_compatible(self, swf_client, domain, name, version):
        """Check if the remote config has the same defaults as this one.

        Raise SWFRegistrationError in case of SWF communication errors or
        incompatibility and ValueError if any configuration values can't be
        converted to the required types.

        :type swf_client: :class:`flowy.swf.client.SWFClient`
        :param swf_client: an implementation or duck typing of `SWFClient`
        :param domain: the domain name where to check the workflow
        :param name: name of the workflow
        :param version: version of the workflow
        """
        return True

    def conf_activity(self, dep_name, version,
                      name=None,
                      task_list=None,
                      heartbeat=None,
                      schedule_to_close=None,
                      schedule_to_start=None,
                      start_to_close=None,
                      serialize_input=None,
                      deserialize_result=None,
                      retry=(0, 0, 0)):
        """Configure an activity dependency for a workflow implementation.

        dep_name is the name of one of the workflow factory arguments
        (dependency). For example:

            class MyWorkflow:
                def __init__(self, a, b):  # Two dependencies: a and b
                    self.a = a
                    self.b = b
                def run(self, n):
                    pass

            cfg = SWFWorkflowConfig(version=1)
            cfg.conf_activity('a', name='MyActivity', version=1)
            cfg.conf_activity('b', version=2, task_list='my_tl')

        For convenience, if the activity name is missing, it will be the same
        as the dependency name.
        """
        if name is None:
            name = dep_name
        proxy_factory = SWFActivityProxyFactory(
            identity=str(dep_name),
            name=str(name),
            version=str(version),
            task_list=str_or_none(task_list),
            heartbeat=heartbeat,
            schedule_to_close=schedule_to_close,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close, 
            serialize_input=serialize_input,
            deserialize_result=deserialize_result,
            retry=retry)
        self.conf_proxy_factory(dep_name, proxy_factory)

    def conf_workflow(self, dep_name, version,
                      name=None,
                      task_list=None,
                      workflow_duration=None,
                      decision_duration=None,
                      child_policy=None,
                      serialize_input=None,
                      deserialize_result=None,
                      retry=(0, 0, 0)):
        """Same as conf_activity but for sub-workflows."""
        if name is None:
            name = dep_name
        proxy_factory = SWFWorkflowProxyFactory(
            identity=str(dep_name),
            name=str(name),
            version=str(version),
            task_list=str_or_none(task_list),
            workflow_duration=workflow_duration,
            decision_duration=decision_duration,
            child_policy=child_policy,
            serialize_input=serialize_input,
            deserialize_result=deserialize_result,
            retry=retry)
        self.conf_proxy_factory(dep_name, proxy_factory)

    def wrap(self, func):
        """Insert an additional DescCounter object for rate limiting."""
        f = super(SWFWorkflowConfig, self).wrap(func)

        @functools.wraps(func)
        def wrapper(input_data, *extra_args):
            extra_args = extra_args + (DescCounter(int(self.rate_limit)), )
            return f(input_data, *extra_args)

        return wrapper


class SWFRegistrationError(Exception):
    """Can't register a task remotely."""
