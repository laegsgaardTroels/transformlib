from transformlib import Transform

from transformlib.exceptions import TransformlibCycleException
from transformlib.exceptions import TransformlibDuplicateTransformException

from typing import Dict
from typing import List
from typing import Optional

from collections import deque
from pathlib import Path
import importlib
import pkgutil
import sys
import time

import logging

logger = logging.getLogger(__name__)


# A graph is a dictionary of transforms (keys) to a list of transformations (value) which have
# outputs that are inputs to given transform (key).
Graph = Dict[Transform, List[Transform]]


class Pipeline:
    """A `Pipeline` is a topologically ordered list of transforms.

    A `Pipeline` can be run from the command line:

    .. highlight:: bash
    .. code-block:: bash

        transform path/to/transforms/*.py
        transform -v path/to/transforms/*.py

    This will topologically sort and run all Transform objects found in the py files and save/load
    the data from the `config.ROOT_DIR`.
    """

    def __init__(self, transforms: Optional[List[Transform]] = None):
        if transforms is None:
            self.transforms = []
        else:
            self.transforms = transforms

    @property
    def tasks(self):
        """A topologically ordered list of transforms."""
        if len(set(self.transforms)) != len(self.transforms):
            raise TransformlibDuplicateTransformException(f"Duplicate {self.transforms}")
        return _get_tasks(self.transforms)  # Topologically sort the transforms.

    def __repr__(self):
        return f"Pipeline({', '.join(map(repr, self.tasks))})"

    def run(self) -> None:
        """Used to run all the transforms in the pipeline."""
        logger.info(f'Beginning running of {self}.')
        start = time.perf_counter()
        for transform in self.tasks:
            transform.run()
        logger.info(f'Completed running of {self} took {time.perf_counter() - start}.')

    @classmethod
    def discover_transforms(cls, *plugins):
        """Find and import all transforms in plugins.

        This function will automatically disover transforms from plugins
        using namespace packages e.g. fixed namespace(s), as defined by
        the input plugins args to the function, where plugins are saved.

        Assuming you have a module called `transforms`. You can then find and
        run all the Transform objects in this module:

        >>> import transforms
        >>> from transformlib.pipeline import Pipeline
        >>> pipeline = Pipeline.discover_transforms(transforms)
        >>> pipeline.run()

        Args:
            *plugins (module): Module(s) that contains transforms.

        Returns:
            Pipeline: A pipeline of the discovered transforms.

        References:
            [1] https://packaging.python.org/guides/creating-and-discovering-plugins/
        """
        transforms = []
        for plugin in plugins:

            if isinstance(plugin, Transform):
                logger.info(f"Discovered {plugin} as plugin input.")
                transforms.append(plugin)
                continue

            for _, name, ispkg in pkgutil.walk_packages(
                path=plugin.__path__,
                prefix=plugin.__name__ + ".",
            ):
                plugin_module = importlib.import_module(name)
                for attrname in dir(plugin_module):
                    new_attr = getattr(plugin_module, attrname)
                    if isinstance(new_attr, Transform):
                        logger.info(f"Discovered {new_attr} in {name}.")
                        transforms.append(new_attr)
        return cls(transforms)

    def add_transforms_from_path(self, plugin_path: Path) -> None:
        """Find and import all transform from a file.

        Args:
            plugin_path (Path): A path to a py file with Transform to
                be added to the Pipeline.
        """
        try:
            sys.path.insert(0, str(plugin_path.parent))
            name = plugin_path.stem
            plugin_module = importlib.import_module(name)
            for attrname in dir(plugin_module):
                new_attr = getattr(plugin_module, attrname)
                if isinstance(new_attr, Transform):
                    logger.info(f"Discovered {new_attr} in {name}.")
                    self.transforms.append(new_attr)
        finally:
            sys.path.pop(0)


def _get_tasks(transforms: List[Transform]) -> List[Transform]:
    """Get the tasks in order of execution.

    Args:
        transforms (List[Transform]): A list of transforms to be topologically ordered.

    Returns:
        List[Transforms]: A topologically ordered list of transforms s.t. the output of
            a transform cannot be the input to a previous transform in the list.
    """
    graph = _create_graph(transforms)
    tasks = _tsort(graph)
    return tasks


def _create_graph(transforms: List[Transform]) -> Graph:
    """Create a graph out of a list of transforms.

    The graph is encoded as a dict of {from: List[to]}, where from and to are
    transforms.

    Args:
        transforms (List[Transform]): A list of transforms.

    Returns:
        Graph: The list of transforms encoded as a graph. A graph is a dictionary of
            transforms (keys) to a list of transformations (value) which have outputs
            that are inputs to given transform (key).
    """
    return {
        from_:
        [
            to for to in transforms
            if any(
                output in to.inputs for output in from_.outputs
            )
        ]
        for from_ in transforms
    }


def _tsort(graph: Graph) -> List[Transform]:
    """Used to sort a directed acyclic graph of tasks in order of execution.

    Args:
        graph (Dict[Transform, List(Transform)]): A list of output transforms
            to inputs.

    Returns:
        List(Transform): A list of transforms sorted in order of execution.

    References:
        [1] https://algocoding.wordpress.com/2015/04/05/topological-sorting-python/
    """
    in_degree = {u: 0 for u in graph}     # determine in-degree
    for u in graph:                       # of each node
        for v in graph[u]:
            in_degree[v] += 1

    Q = deque()                 # collect nodes with zero in-degree
    for u in in_degree:
        if in_degree[u] == 0:
            Q.appendleft(u)

    L = []     # list for order of nodes

    while Q:
        u = Q.pop()          # choose node of zero in-degree
        L.append(u)          # and 'remove' it from graph
        for v in graph[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                Q.appendleft(v)

    if len(L) == len(graph):
        return L
    else:                    # if there is a cycle,
        raise TransformlibCycleException("Cycle detected in the DAG.")
