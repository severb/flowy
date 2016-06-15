import boto3

from flowy.utils import str_or_none

__all__ = ['CHILD_POLICY', 'DURATION', 'IDENTITY_SIZE', 'SWFClient']


# poor man's enums and constants
class CHILD_POLICY:
    TERMINATE = 'TERMINATE'
    REQUEST_CANCEL = 'REQUEST_CANCEL'
    ABANDON = 'ABANDON'

    ALL = ('TERMINATE', 'REQUEST_CANCEL', 'ABANDON')


class DURATION:
    INDEF = 'NONE'
    ONE_YEAR = 31622400  # seconds in a leap year; 60 * 60 * 24 * 366

    ALL = ('NONE', 31622400)


IDENTITY_SIZE = 256


class SWFClient(object):
    """A thin wrapper around :func:`boto3.client('swf')` for sanitizing
    parameters and maybe error handling. This will be interfacing in the
    :mod:`flowy.swf` for communicating to AWS SWF. Custom clients may be used,
    interfacing this class.
    """

    def __init__(self, client=None, config=None, kwargs=None):
        """Setup initial swf client. Can inject an initialized SWF client,
        ignoring the additional config or config can be passed to create the
        SWF client.

        Additional keyword arguments can be passed to initialise the low level
        client. The first arg (service_name) is by default 'swf' and kwarg
        config from :param:`config`; this can be overwritten.
        See :py:meth:`boto3.session.Session.client`.

        :type client: botocore.client.BaseClient
        :param client: a low-level service client for swf.
            See :py:meth:`boto3.session.Session.client`

        :type config: botocore.client.Config
        :param config: custom config to instantiate the 'swf' client

        :type kwargs: dict
        :param kwargs: kwargs for passing to client initialisation. The config
            param can be overwritten here
        """
        kwargs = kwargs if isinstance(kwargs, dict) else {}
        kwargs.setdefault('config', config)

        self.client = client or boto3.client('swf', **kwargs)

    def start_workflow_execution(self, domain, wid, name, version,
                                 input=None, priority=None, task_list=None,
                                 execution_start_to_close_timeout=None,
                                 task_start_to_close_timeout=None,
                                 child_policy=None, tags=None,
                                 lambda_role=None):
        """Wrapper for `boto3.client('swf').start_workflow_execution`.

        :raises: `botocore.exceptions.ClientError`
        """
        kwargs = {
            'domain': str_or_none(domain),
            'workflowId': str_or_none(wid),
            'workflowType': {
                'name': str_or_none(name),
                'version': str_or_none(version)
            },
            'input': str_or_none(input),
            'taskPriority': str_or_none(priority),
            'taskList': {
                'name': str_or_none(task_list)
            },
            'executionStartToCloseTimeout': str_or_none(execution_start_to_close_timeout),
            'taskStartToCloseTimeout': str_or_none(task_start_to_close_timeout),
            'childPolicy': cp_encode(child_policy),
            'tagList': tags_encode(tags),
            'lambda_role': str_or_none(lambda_role)
        }
        normalize_data(kwargs)
        response = self.client.start_workflow_execution(**kwargs)
        return response

    def poll_for_decision_task(self, domain, task_list, identity=None,
                               next_page_token=None, max_page_size=1000,
                               reverse_order=False):
        """Wrapper for `boto3.client('swf').poll_for_decision_task`.

        :raises: `botocore.exceptions.ClientError`, AssertionError
        """
        assert max_page_size <= 1000, 'Page size greater than 1000.'
        identity = str(identity)[:IDENTITY_SIZE] if identity else identity

        kwargs = {
            'domain': str_or_none(domain),
            'taskList': {
                'name': str_or_none(task_list)
            },
            'identity': identity,
            'nextPageToken': str_or_none(next_page_token),
            'maximumPageSize': max_page_size,
            'reverseOrder': reverse_order
        }
        normalize_data(kwargs)
        response = self.client.poll_for_decision_task(**kwargs)
        return response

    def poll_for_activity_task(self, domain, task_list, identity=None):
        """Wrapper for `boto3.client('swf').poll_for_activity_task`.

        :raises: `botocore.exceptions.ClientError`
        """
        identity = str(identity)[:IDENTITY_SIZE] if identity else identity

        kwargs = {
            'domain': str_or_none(domain),
            'taskList': {
                'name': str_or_none(task_list),
            },
            'identity': identity,
        }
        normalize_data(kwargs)
        response = self.client.poll_for_activity_task(**kwargs)
        return response

    def record_activity_task_heartbeat(self, task_token, details=None):
        """Wrapper for `boto3.client('swf').record_activity_task_heartbeat`.

        :raises: `botocore.exceptions.ClientError`
        """
        kwargs = {
            'taskToken': str_or_none(task_token),
            'details': str_or_none(details),
        }
        normalize_data(kwargs)
        response = self.client.record_activity_task_heartbeat(**kwargs)
        return response

    def respond_activity_task_failed(self, task_token, reason=None, details=None):
        """Wrapper for `boto3.client('swf').respond_activity_task_failed`.

        :raises: `botocore.exceptions.ClientError`
        """
        kwargs = {
            'taskToken': str_or_none(task_token),
            'reason': str_or_none(reason),
            'details': str_or_none(details)
        }
        normalize_data(kwargs)
        response = self.client.respond_activity_task_failed(**kwargs)
        return response

    def respond_activity_task_completed(self, task_token, result=None):
        """Wrapper for `boto3.client('swf').respond_activity_task_completed`.

        :raises: `botocore.exceptions.ClientError`
        """
        kwargs = {
            'taskToken': str_or_none(task_token),
            'result': str_or_none(result)
        }
        normalize_data(kwargs)
        response = self.client.respond_activity_task_completed(**kwargs)
        return response

    def respond_decision_task_completed(self, task_token, decisions=None,
                                        exec_context=None):
        """Wrapper for `boto3.client('swf').respond_decision_task_completed`.

        :raises: `botocore.exceptions.ClientError`
        """
        kwargs = {
            'taskToken': str_or_none(task_token),
            'decisions': decisions or [],
            'executionContext': str_or_none(exec_context)
        }
        normalize_data(kwargs)
        response = self.client.respond_decision_task_completed(**kwargs)
        return response


def normalize_data(d):
    """Recursively goes through method kwargs and removes default values. This
    has side effect on the input.

    :type d: dict
    :param d: method's kwargs with default values to be removed
    """
    e = {}
    for key in list(d.keys()):
        if isinstance(d[key], dict):
            normalize_data(d[key])
        if d[key] in (None, e):
            del d[key]


def cp_encode(val):
    """Encodes and ensures value to a valid child policy.

    :param val: a valid child policy that has a `str` representation
    :rtype: str
    """
    if val is not None:
        val = str(val).upper()
        if val not in CHILD_POLICY.ALL:
            raise ValueError('Invalid child policy value: {}. Valid policies'
                             ' are {}.'.format(val, CHILD_POLICY.ALL))
    return val


def duration_encode(val, name, limit=None):
    """Encodes and validates duration used in several request parameters. For an
    indefinitely period use `DURATION.INDEF`.

    :param val: duration in strictly positive integer seconds or
        `DURATION.INDEF`/'NONE' for indefinitely
    :param name: parameter name to be encoded, used in error description
    :param limit: the upper limit to validate the duration in seconds
    :rtype: str
    """
    s_val = str(val).upper() if val else None
    if val is None or s_val == DURATION.INDEF:
        return val

    limit = limit or float('inf')
    err = ValueError('The value of {} must be a strictly positive integer and '
                     'lower than {} or "NONE": {} '.format(name, limit, val))
    try:
        val = int(val)
    except ValueError:
        raise err
    if not 0 < val < limit:
        raise err

    return s_val


def tags_encode(tags):
    """Encodes the list of tags. Trimmed to maximum 5 tags.

    :param tags: the list of tags; max 5 will be taken
    :rtype: list of str
    """
    if tags is None:
        return None
    return list(set(str(t) for t in tags))[:5]
