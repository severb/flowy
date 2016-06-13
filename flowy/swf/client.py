import boto3

from flowy.utils import str_or_none

__all__ = ['CHILD_POLICY', 'DURATION', 'SWFClient']


# poor man's enums
class CHILD_POLICY:
    TERMINATE = 'TERMINATE'
    REQUEST_CANCEL = 'REQUEST_CANCEL'
    ABANDON = 'ABANDON'

    ALL = ('TERMINATE', 'REQUEST_CANCEL', 'ABANDON')


class DURATION:
    INDEF = 'NONE'
    ONE_YEAR = 31622400  # seconds in a leap year; 60 * 60 * 24 * 366

    ALL = ('NONE', 31622400)


class SWFClient(object):
    """A thin wrapper around `boto3.client` for sanitizing parameters and maybe
    error handling. This will be interfacing in the `boto.swf` for communicating
    to AWS SWF. Custom clients may be used, interfacing this class.
    """

    def __init__(self, swf_client=None, config=None):
        """Setup initial swf client. Can inject an initialized SWF client,
        ignoring the additional config or config can be passed to create the
        SWF client.

        :type swf_client: botocore.client.BaseClient
        :param swf_client: a low-level service client for swf.
                            See :py:meth:`boto3.session.Session.client`

        :type config: botocore.client.Config
        :param config: custom config to instantiate the 'swf' client
        """
        self.client = swf_client or boto3.client('swf', config=config)

    def start_workflow_execution(self, domain, wid, name, version,
                                 input=None, task_priority=None, task_list=None,
                                 execution_start_to_close_timeout=None,
                                 task_start_to_close_timeout=None,
                                 child_policy=None, tags=None,
                                 lambda_role=None):
        """Wrapper for `boto3.client('swf').start_workflow_execution()`.

        :raises: `botocore.exceptions.ClientError`
        """
        kwargs = {
            'domain': str(domain),
            'workflowId': str(wid),
            'workflowType': {
                'name': str(name),
                'version': str(version)
            },
            'input': str_or_none(input),
            'taskPriority': str_or_none(task_priority),
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
    """Encodes the list of tags. Maximum 5 tags allowed.

    :param tags: the list of tags; max 5 will be taken
    :rtype: list of str
    """
    if tags is None:
        return None
    return list(set(str(t) for t in tags))[:5]
