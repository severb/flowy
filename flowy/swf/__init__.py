from collections import namedtuple


def SWFTaskId(name, version):
    return _SWFTaskId(str(name), str(version))

_SWFTaskId = namedtuple('_SWFTaskId', 'name version')
