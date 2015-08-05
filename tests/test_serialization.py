import pytest

def make_traverse_cases():
    # pytest attempts to look for tests derefing the results, causing trouble
    from flowy.result import error, placeholder, result

    r0 = result(u'r0', 0)
    e1 = error('err1', 1)
    e2 = error('err2', 2)
    e3 = error('err3', 3)
    r4 = result(4, 4)
    ph = placeholder()

    return (
        ([], ([], tuple())),
        (1, (1, (1,))),
        (u'abc', (u'abc', (u'abc',))),
        (r0, (u'r0', (u'r0',))),
        (
            [e1, [e2, e3], (r4, r0), [1, 2, [3]], {r4: ['xyz', ph]}],
            (
                [e1, [e2, e3], [4, u'r0'], [1, 2, [3]], {4: ['xyz', ph]}],
                (e1, e2, e3, 4, u'r0', 1, 2, 3, 4, 'xyz', ph)
            )
        ), (
            [{(r4, tuple()): [r0, (e1, ph), tuple()]}],
            (
                [{(4, tuple()): [u'r0', [e1, ph], []]}],
                (4, u'r0', e1, ph)
            )
        )
    )


@pytest.fixture
def traverse():
    """The traverse function with a flat tuple reducer."""
    from flowy.serialization import traverse_data
    from functools import partial
    def flat(result, n):
        return result + (n,)
    return partial(traverse_data, f=flat, initial=tuple())


@pytest.mark.parametrize(['value', 'result'], make_traverse_cases())
def test_traverse_data(traverse, value, result):
    assert traverse(value) == result


def test_traverse_recursive(traverse):
    a = {'x': []}
    a['x'].append([a])
    with pytest.raises(ValueError):
        traverse(a)


def test_traverse_unsized(traverse):
    with pytest.raises(ValueError):
        traverse(x for x in range(10))


def make_err_and_ph_cases():
    from flowy.result import error, placeholder, result

    r0 = result(u'r0', 0)
    e1 = error('err1', 1)
    e2 = error('err2', 2)
    ph = placeholder()

    return (
        (((None, False), 1), (None, False)),
        (((None, False), e1), (e1, False)),
        (((None, False), ph), (None, True)),
        (((e1, False), e2), (e1, False)),
        (((e2, False), e1), (e1, False)),
        (((e1, False), ph), (e1, True)),
    )


@pytest.mark.parametrize(['value', 'result'], make_err_and_ph_cases())
def test_check_err_and_placeholders(value, result):
    from flowy.serialization import check_err_and_placeholders
    r, v = value
    assert check_err_and_placeholders(r, v) == result


def make_collect_cases():
    from flowy.result import error, placeholder, result

    r0 = result(u'r0', 0)
    e1 = error('err1', 1)
    e2 = error('err2', 2)
    ph = placeholder()

    return (
        (((None, None), 1), (None, None)),
        (((None, None), ph), (None, None)),
        (((None, None), e1), (e1, None)),
        (((None, None), e2), (e2, None)),
        (((e2, None), e1), (e1, None)),
        (((None, None), r0), (None, [r0])),
    )


@pytest.mark.parametrize(['value', 'result'], make_collect_cases())
def test_collect_err_and_results(value, result):
    from flowy.serialization import collect_err_and_results
    r, v = value
    assert collect_err_and_results(r, v) == result



import uuid
x_uuid = uuid.uuid4()

@pytest.mark.parametrize(['value', 'result'], (
    (1, 1),
    ([], []),
    (tuple(), []),
    ([1, (2, 3), {'4': b'5', u'6': x_uuid}], [1, [2, 3], {'4': b'5', u'6': x_uuid}])
))
def test_dumps_loads(value, result):
    from flowy.serialization import dumps, loads
    assert loads(dumps(value)) == result
