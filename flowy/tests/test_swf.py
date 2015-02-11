from flowy.tests.workflows import *


def TestCase(workflow, test_cases):
    pass


TestCase(QuickReturn, [
    {
        'input_args': [10],
        'expected': {
            'finish': 10,
        },
    },
    {
        'input_args': ['abc'],
        'expected': {
            'finish': 'abc',
        },
    },
    {
        'input_args': [[1, 2, 3]],
        'expected': {
            'finish': [1, 2, 3],
        },
    },
])


TestCase(Closure, [
    {
        'input_args': [1],
        'expected': {
            'finish': 1,
        },
    },
])


TestCase(Arguments, [
    {
        'input_args': ['a', 'b'],
        'expected': {
            'finish': ['a', 'b', 1, 2],
        },
    },
    {
        'input_args': ['a', 'b', 'c'],
        'expected': {
            'finish': ['a', 'b', 'c', 2],
        },
    },
    {
        'input_args': ['a', 'b'],
        'input_kwargs': {'d': 'd'},
        'expected': {
            'finish': ['a', 'b', 3, 'd'],
        },
    },
])


TestCase(Dependency, [
    {
        'input_args': [5],
        'expected': {
            'schedule': [
                {
                    'name': 'inc-0-0',
                    'input_args': [0],
                },
            ],
        },
    },
    {
        'input_args': [5],
        'results': {
            'inc-0-0': 1,
            'inc-1-0': 2,
        },
        'expected': {
            'schedule': [
                {
                    'name': 'inc-2-0',
                    'input_args': [2],
                },
            ],
        },
    },
    {
        'input_args': [5],
        'results': {
            'inc-0-0': 1,
            'inc-1-0': 2,
            'inc-2-0': 3,
            'inc-3-0': 4,
            'inc-4-0': 5,
        },
        'expected': {
            'finish': 5,
        },
    },
])


TestCase(Parallel, [
    {
        'input_args': [4],
        'expected': {
            'schedule': [
                {
                    'name': 'inc-0-0',
                    'input_args': [0],
                },
                {
                    'name': 'inc-1-0',
                    'input_args': [1],
                },
                {
                    'name': 'inc-2-0',
                    'input_args': [2],
                },
                {
                    'name': 'inc-3-0',
                    'input_args': [3],
                },
            ],
        },
    },
    {
        'input_args': [4],
        'running': [
            'inc-0-0',
            'inc-2-0',
            'inc-3-0',
        ],
        'results': {
            'inc-1-0': 2,
        },
    },
])
