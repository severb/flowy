import heapq
import itertools

from flowy.result import is_result_proxy
from flowy.result import result
from flowy.utils import i_or_args
from flowy.utils import sentinel


__all__ = ['first', 'finish_order', 'parallel_reduce']


def _order_key(i):
    return i.__factory__


def first(result, *results):
    """Return the first result finish from a list of results.

    If no one is finished yet - all of the results are placeholders - return
    the first placeholder from the list.
    """
    rs = []
    for r in i_or_args(result, results):
        if is_result_proxy(r):
            rs.append(r)
        else:
            return r
    return min(rs, key=_order_key)


def finish_order(result, *results):
    """Return the results in their finish order.

    The results that aren't finished yet will be at the end with their relative
    order preserved.
    """
    rs = []
    for r in i_or_args(result, results):
        if is_result_proxy(r):
            rs.append(r)
        else:
            yield r
    for r in sorted(rs, key=_order_key):
        yield r


def parallel_reduce(f, iterable, initializer=sentinel):
    """Like reduce() but optimized to maximize parallel execution.

    The reduce function must be associative and commutative.

    The reduction will start as soon as two results are available, regardless
    of their "position". For example, the following reduction is possible:

     5 ----1-----|
    15           --------------4----------|
    15           |                        -------------12|
    15           |                        |              -------------17|
  R 15           |                        |              |              -------------21
    15 ----------|---2-----|              |              |              |
    15           |         --------------8|              |              |
    10 ---------3|         |                             |              |
    60 --------------------|-----------------------------|--------4-----|
    50 --------------------|----------------------------5|
    20 -------------------6|

    The iterable must have at least one element, otherwise a ValueError will be
    raised.

    The improvement over the built-in reduce() is obtained by starting the
    reduction as soon as any two results are available. The number of reduce
    operations is always constant and equal to len(iterable) - 1 regardless of
    how the reduction graph looks like.
    """
    if initializer is not sentinel:
        iterable = itertools.chain([initializer], iterable)
    results, non_results = [], []
    for x in iterable:
        if is_result_proxy(x):
            results.append(x)
        else:
            non_results.append(x)
    i = iter(non_results)
    reminder = sentinel
    for x in i:
        try:
            y = next(i)
            results.append(f(x, y))
        except StopIteration:
            reminder = x
            if not results:  # len(iterable) == 1
                # Wrap the value in a result for uniform interface
                return result(x, -1)
    if not results:  # len(iterable) == 0
        raise ValueError(
            'parallel_reduce() of empty sequence with no initial value')
    if is_result_proxy(results[0]):
        results = [(r.__factory__, r) for r in results]
        heapq.heapify(results)
        return _parallel_reduce_recurse(f, results, reminder)
    else:
        # Looks like we don't use a task for reduction, fallback on reduce
        return reduce(f, results)


def _parallel_reduce_recurse(f, results, reminder=sentinel):
    if reminder is not sentinel:
        _, first = heapq.heappop(results)
        new_result = f(reminder, first)
        heapq.heappush(results, (new_result.__factory__, new_result))
        return _parallel_reduce_recurse(f, results)
    _, x = heapq.heappop(results)
    try:
        _, y = heapq.heappop(results)
    except IndexError:
        return x
    new_result = f(x, y)
    heapq.heappush(results, (new_result.__factory__, new_result))
    return _parallel_reduce_recurse(f, results)
