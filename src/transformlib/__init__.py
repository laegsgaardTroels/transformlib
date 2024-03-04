from pathlib import Path
import sys
import importlib
import time
import graphlib
from typing import Callable, Any

import logging

logger = logging.getLogger(__name__)


config = {"data_dir": Path(__file__).parent / "data"}


class TransformlibCycleException(Exception):
    """Raised when there is a cycle in the Pipeline."""


class TransformlibDuplicateTransformException(Exception):
    """Raised when there is duplicate Transform in the Pipeline."""


class TransformlibDuplicateInputException(Exception):
    """Raised when there is duplicate Input in the Pipeline."""


class TransformlibDuplicateOutputException(Exception):
    """Raised when there is duplicate Output in the Pipeline."""


class Node:
    """The Node base class is a node in a directed asyclic graph (DAG) of data transformations."""

    def __init__(self, relative_path: Path | str):
        self.relative_path = relative_path

    @property
    def data_dir(self) -> Path:
        return Path(config["data_dir"])

    @property
    def path(self) -> Path:
        """The path to the node in the directory :py:const:`config.DATA_DIR`."""
        return self.data_dir / self.relative_path

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.relative_path})"

    def __eq__(self, other) -> bool:
        return self.path == other.path

    def __hash__(self) -> int:
        return hash(self.path)


class Output(Node):
    """An Output is a sink in a DAG of data transformations."""

    pass


class Input(Node):
    """An Input is a source in a DAG of data transformations."""

    pass


Func = Any


class Transform:
    """Used to organize transformations of data.

    A :py:class:`transformlib.Transform` is a many to many mapping between
    :py:class:`transformlib.Input` and :py:class:`transformlib.Output` nodes.

    A Transform is often constructed using the :py:func:`transformlib.transform` decorator:

    .. highlight:: python
    .. code-block:: python

        import json
        from transformlib import transform, Output, Input


        @transform(
            json_output=Output('mapping.json'),
            txt_input=Input('mapping.txt'),
        )
        def convert_to_json(json_output, txt_input):
            text = txt_input.path.read_text()
            mapping = dict(map(lambda line: line.split(','), text.splitlines()))
            json_output.path.write_text(json.dumps(mapping, indent=4))
    """

    def __init__(
        self,
        output_kwargs: dict[str, Output],
        func: Func,
        input_kwargs: dict[str, Input],
    ):
        self.output_kwargs = output_kwargs
        self.func = func
        self.input_kwargs = input_kwargs
        if len(set(self.inputs)) != len(self.inputs):
            raise TransformlibDuplicateInputException(f"Duplicate inputs={self.inputs}")
        if len(set(self.outputs)) != len(self.outputs):
            raise TransformlibDuplicateOutputException(
                f"Duplicate outputs={self.outputs}"
            )

    @property
    def outputs(self) -> list[Output]:
        """A tuple with all the :py:class:`transformlib.Output` of the Transform."""
        return list(self.output_kwargs.values())

    @property
    def inputs(self) -> list[Input]:
        """A tuple with all the :py:class:`transformlib.Input` to the Transform."""
        return list(self.input_kwargs.values())

    @property
    def nodes(self) -> list[Input | Output]:
        """All :py:class:`transformlib.Output` and :py:class:`transformlib.Input`."""
        return self.outputs + self.inputs

    def run(self) -> None:
        """Runs the Transform."""
        logger.info(f"Beginning running of {self}.")
        start = time.perf_counter()
        self(**self.output_kwargs, **self.input_kwargs)
        logger.info(f"Completed running of {self} took {time.perf_counter() - start}.")

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + ", ".join(
                map(
                    lambda key: key + "=" + repr(self.output_kwargs[key]),
                    self.output_kwargs,
                )
            )
            + (", " if self.input_kwargs else "")
            + ", ".join(
                map(
                    lambda key: key + "=" + repr(self.input_kwargs[key]),
                    self.input_kwargs,
                )
            )
            + ")"
        )

    def __str__(self) -> str:
        return self.func.__name__

    def __eq__(self, other) -> bool:
        if set(self.nodes) == set(other.nodes):
            return True
        return False

    def __hash__(self) -> int:
        return hash(tuple(self.nodes))


def transform(**kwargs: Input | Output) -> Callable[[Func], Transform]:
    """Convert a function to a :py:class:`transformlib.Transform`.

    Args:
        **kwargs (Dict[str, Node]): The :py:class:`transformlib.Input`
            and :py:class:`transformlib.Output` nodes of the transform.

    Returns:
        function: A decorator that returns a Transform object.
    """

    def decorator(func) -> Transform:
        return Transform(
            output_kwargs={
                key: value for key, value in kwargs.items() if isinstance(value, Output)
            },
            func=func,
            input_kwargs={
                key: value for key, value in kwargs.items() if isinstance(value, Input)
            },
        )

    return decorator


class Pipeline:
    """A Pipeline is a topologically ordered list of :py:class:`transformlib.Transform` objects.

    A :py:class:`transformlib.Pipeline` can be run from the command line:

    .. highlight:: bash
    .. code-block:: bash

        transform path/to/transforms/*.py
        transform -v path/to/transforms/*.py

    This will topologically sort and run all :py:class:`transformlib.Transform` objects found in
    the py files.
    """

    def __init__(self, transforms: list[Transform] | None = None):
        if transforms is None:
            self.transforms = []
        else:
            self.transforms = transforms
        if len(set(self.transforms)) != len(self.transforms):
            raise TransformlibDuplicateTransformException(
                f"Duplicate transforms={self.transforms}"
            )

    @property
    def tasks(self) -> list[Transform]:
        tsort = graphlib.TopologicalSorter()
        for transform in self.transforms:
            predecessors = [
                t
                for t in self.transforms
                if any(o in transform.inputs for o in t.outputs)
            ]
            tsort.add(transform, *predecessors)
        try:
            return list(tsort.static_order())
        except graphlib.CycleError as exception:
            raise TransformlibCycleException("Cycle detected.") from exception

    @property
    def outputs(self) -> list[Output]:
        """A tuple with all the Output nodes of the Pipeline."""
        return [node for transform in self.transforms for node in transform.outputs]

    @property
    def inputs(self) -> list[Input]:
        """A tuple with all the Input nodes of the Pipeline."""
        return [node for transform in self.transforms for node in transform.inputs]

    @property
    def nodes(self) -> list[Node]:
        """A tuple with all the Node objects of the Pipeline."""
        return [node for transform in self.transforms for node in transform.nodes]

    def run(self) -> None:
        """Used to run all the :py:class:`transformlib.Transform` objects in the pipeline."""
        logger.info(f"Beginning running of {self}.")
        start = time.perf_counter()
        for transform in self.tasks:
            transform.run()
        logger.info(f"Completed running of {self} took {time.perf_counter() - start}.")

    @classmethod
    def from_paths(cls, paths: list[str] | list[Path]):
        transforms = []
        for path in list(map(Path, paths)):
            if path.stem.startswith("__"):
                continue
            if str(path.parent.resolve()) not in sys.path:
                sys.path.append(str(path.parent.resolve()))

            plugin_module = importlib.import_module(path.stem)
            for attrname in dir(plugin_module):
                attr = getattr(plugin_module, attrname)
                if isinstance(attr, Transform):
                    logger.info(f"Discovered Transform: {attr} in {path}")
                    transforms.append(attr)
        return cls(transforms)

    def __len__(self) -> int:
        return len(self.transforms)


__version__ = "0.2.7"
__all__ = [
    "Node",
    "Output",
    "Input",
    "Transform",
    "transform",
    "Pipeline",
    "config",
    "__version__",
]
