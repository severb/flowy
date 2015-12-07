import copy
import tempfile
import warnings
import webbrowser

from flowy.operations import first
from flowy.proxy import Proxy
from flowy.result import is_result_proxy
from flowy.serialization import collect_err_and_results
from flowy.serialization import traverse_data
from flowy.utils import logger
from flowy.utils import short_repr


__all__ = ['TracingProxy', 'ExecutionTracer']


# XXX: Trace dependencies even if data structures containing result proxies are used
class TracingProxy(Proxy):
    """Similar to a BoundProxy but records task dependency.

    This works by checking every arguments passed to the proxy for task results
    and records the dependency between this call and the previous ones
    generating the task results. It also adds some extra information on the
    task result itself for tracking purposes.

    This can be used with ExecutionTracer to track the execution dependency and
    display in in different forms for analysis.
    """

    def __init__(self, tracer, trace_name, *args, **kwargs):
        super(TracingProxy, self).__init__(*args, **kwargs)
        self.trace_name = trace_name
        self.tracer = tracer

    def __call__(self, *args, **kwargs):
        node_id = "%s-%s" % (self.trace_name, self.call_number)
        ((t_args, t_kwargs), (err, results)) = traverse_data(
            [args, kwargs], f=collect_err_and_results, initial=(None, None)
        )
        r = super(TracingProxy, self).__call__(*t_args, **t_kwargs)
        assert is_result_proxy(r)
        factory = r.__factory__
        factory.node_id = node_id
        if err is not None:
            self.tracer.schedule_activity(node_id, self.trace_name)
            self.tracer.flush_scheduled()
            error_factory = err.__factory__
            self.tracer.error(node_id, str(error_factory.value))
        for dep in results or []:
            self.tracer.add_dependency(dep.__factory__.node_id, node_id)
        return r


class ExecutionTracer(object):
    """Record the execution history for display and analysis."""

    def __init__(self):
        self.reset()

    def schedule_activity(self, node_id, name):
        assert node_id not in self.nodes
        self.nodes[node_id] = name
        self.current_schedule.append(node_id)
        self.timeouts[node_id] = 0
        self.activities.add(node_id)

    def schedule_workflow(self, node_id, name):
        assert node_id not in self.nodes
        self.nodes[node_id] = name
        self.current_schedule.append(node_id)
        self.timeouts[node_id] = 0

    def flush_scheduled(self):
        self.levels.append(self.current_schedule)
        self.current_schedule = []

    def result(self, node_id, result):
        assert node_id in self.nodes
        assert node_id not in self.levels
        self.levels.append(node_id)
        self.results[node_id] = result

    def error(self, node_id, reason):
        assert node_id in self.nodes
        assert node_id not in self.levels
        self.levels.append(node_id)
        self.errors[node_id] = reason

    def timeout(self, node_id):
        assert node_id in self.nodes
        assert node_id not in self.results or node_id not in self.errors
        self.timeouts[node_id] += 1

    def add_dependency(self, from_node, to_node):
        """ node_id -> node_id """
        self.deps.setdefault(from_node, []).append(to_node)

    def copy(self):
        et = ExecutionTracer()
        et.__dict__ = copy.deepcopy(self.__dict__)
        return et

    def reset(self):
        self.levels = []
        self.current_schedule = []
        self.timeouts = {}
        self.results = {}
        self.errors = {}
        self.activities = set()
        self.deps = {}
        self.nodes = {}

    def to_dot(self):
        """Render the dot for the recorded execution."""
        try:
            import pygraphviz as pgv
        except ImportError:
            warnings.warn('Extra requirements for "trace" are not available.')
            return
        graph = pgv.AGraph(directed=True, strict=False)

        hanging = set()
        for node_id, node_name in self.nodes.items():
            shape = 'box'
            if node_id in self.activities:
                shape = 'ellipse'
            finish_id = 'finish-%s' % node_id
            color, fontcolor = 'black', 'black'
            if node_id in self.errors:
                color, fontcolor = 'red', 'red'
            graph.add_node(node_id, label=node_name, shape=shape, width=0.8,
                           color=color, fontcolor=fontcolor)
            if node_id in self.results or node_id in self.errors:
                if node_id in self.errors:
                    rlabel = str(self.errors[node_id])
                else:
                    rlabel = short_repr.repr(self.results[node_id])
                    rlabel = ' ' + '\l '.join(rlabel.split('\n'))  # Left align
                graph.add_node(finish_id, label='', shape='point', width=0.1, color=color)
                graph.add_edge(node_id, finish_id, arrowhead='none', penwidth=3, fontsize=8,
                               color=color, fontcolor=fontcolor, label='  ' + rlabel)
            else:
                hanging.add(node_id)

        levels = ['l%s' % i for i in range(len(self.levels))]
        for l in levels:
            graph.add_node(l, shape='point', label='', width=0.1, style='invis')
        if levels:
            start = levels[0]
            for l in levels[1:]:
                graph.add_edge(start, l, style='invis')
                start = l

        for l_id, l in zip(levels, self.levels):
            if isinstance(l, list):
                graph.add_subgraph([l_id] + l, rank='same')
            else:
                graph.add_subgraph([l_id, 'finish-%s' % l], rank='same')

        for from_node, to_nodes in self.deps.items():
            if from_node in hanging:
                hanging.remove(from_node)
            color = 'black'
            style = ''
            if from_node in self.errors:
                color = 'red'
                from_node = 'finish-%s' % from_node
            elif from_node in self.results:
                from_node = 'finish-%s' % from_node
            else:
                style = 'dotted'
            for to_node in to_nodes:
                graph.add_edge(from_node, to_node, color=color, style=style)

        if hanging:
            for node_id in hanging:
                finish_id = 'finish-%s' % node_id
                graph.add_node(finish_id, label='', shape='point', width=0.1, style='invis')
                graph.add_edge(node_id, finish_id, style='dotted', arrowhead='none')
            # l_id is the last level here
            graph.add_subgraph([l_id] + ['finish-%s' % h for h in hanging], rank='same')

        for node_id in self.nodes:
            retries = self.timeouts[node_id]
            if retries:
                graph.add_edge(node_id, node_id, label=' %s' % retries, color='orange',
                               fontcolor='orange', fontsize=8)

        return graph

    def display(self):
        """Create a temp file and render the dot in it."""
        graph = self.to_dot()
        if not graph:
            return
        tf = tempfile.NamedTemporaryFile(mode='w+b', prefix='dot_', suffix='.svg', delete=False)
        graph.draw(tf.name, format='svg', prog='dot')
        logger.info('Workflow execution traced: %s', tf.name)
        webbrowser.open(tf.name)
