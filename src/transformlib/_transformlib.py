from pathlib import Path
import sys
import importlib
import time
import graphlib
import typing

import logging

logger = logging.getLogger(__name__)


config = {"data_dir": "/tmp/"}


class TransformlibCycleException(graphlib.CycleError):
    """Raised when there is a cycle in the Pipeline."""


class TransformlibDuplicateTransformException(Exception):
    """Raised when there is duplicate Transform in the Pipeline."""


class TransformlibDuplicateInputException(Exception):
    """Raised when there is duplicate Input in the Pipeline."""


class TransformlibDuplicateOutputException(Exception):
    """Raised when there is duplicate Output in the Pipeline."""


class Node:
    """The Node base class is a node in a directed asyclic graph of data transformations."""

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
    """An Output is a sink in a directed asyclic graph of data transformations."""

    pass


class Input(Node):
    """An Input is a source in a directed asyclic graph of data transformations."""

    pass


class Parameter:
    def __init__(self, value):
        self.value = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)


Function = typing.Any


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
        function: Function,
        input_kwargs: dict[str, Input],
        parameter_kwargs: dict[str, Parameter] | None = None,
    ):
        self.output_kwargs = output_kwargs
        self.function = function
        self.input_kwargs = input_kwargs
        self.parameter_kwargs = {} if parameter_kwargs is None else parameter_kwargs

        if len(set(self.inputs)) != len(self.inputs):
            raise TransformlibDuplicateInputException(f"Duplicate inputs={self.inputs}")
        if len(set(self.outputs)) != len(self.outputs):
            raise TransformlibDuplicateOutputException(
                f"Duplicate outputs={self.outputs}"
            )

    @property
    def outputs(self) -> list[Output]:
        """A tuple with all the :py:class:`transformlib.Output`(s) of the :py:class:`transformlib.Transform`."""
        return list(self.output_kwargs.values())

    @property
    def inputs(self) -> list[Input]:
        """A tuple with all the :py:class:`transformlib.Input`(s) to the :py:class:`transformlib.Transform`."""
        return list(self.input_kwargs.values())

    @property
    def nodes(self) -> list[Input | Output]:
        """All :py:class:`transformlib.Output`(s) and :py:class:`transformlib.Input`(s)."""
        return self.outputs + self.inputs

    def run(self) -> None:
        """Runs the Transform."""
        logger.info(f"Beginning running of {self}")
        start = time.perf_counter()
        self(**self.output_kwargs, **self.input_kwargs, **self.parameter_kwargs)
        logger.info(f"Completed running of {self} took {time.perf_counter() - start}")

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + kwargs_repr(self.output_kwargs)
            + kwargs_repr(self.input_kwargs)
            + kwargs_repr(self.parameter_kwargs)
            + ")"
        )

    def __str__(self) -> str:
        return self.function.__name__

    def __eq__(self, other) -> bool:
        if set(self.nodes) == set(other.nodes):
            return True
        return False

    def __hash__(self) -> int:
        return hash(tuple(self.nodes))


def kwargs_repr(kwargs: dict[str, typing.Any]) -> str:
    return ", ".join(
        map(
            lambda key: key + "=" + repr(kwargs[key]),
            kwargs,
        )
    )


def transform(
    **kwargs: Input | Output | Parameter,
) -> typing.Callable[[Function], Transform]:
    """Convert a function to a :py:class:`transformlib.Transform`.

    Args:
        **kwargs (dict[str, Node]): The :py:class:`transformlib.Input`
            and :py:class:`transformlib.Output` nodes of the transform.

    Returns:
        Callable[[Function], Transform]: A decorator that returns a Transform object.
    """

    def decorator(function: Function) -> Transform:
        return Transform(
            output_kwargs={
                key: value for key, value in kwargs.items() if isinstance(value, Output)
            },
            function=function,
            input_kwargs={
                key: value for key, value in kwargs.items() if isinstance(value, Input)
            },
            parameter_kwargs={
                key: value
                for key, value in kwargs.items()
                if isinstance(value, Parameter)
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
    the .py files.
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
        """A tuple with all the :py:class:`transformlib.Output` of the :py:class:`transformlib.Pipeline`."""
        return [node for transform in self.transforms for node in transform.outputs]

    @property
    def inputs(self) -> list[Input]:
        """A tuple with all the :py:class:`transformlib.Input` of the :py:class:`transformlib.Pipeline`."""
        return [node for transform in self.transforms for node in transform.inputs]

    @property
    def nodes(self) -> list[Input | Output]:
        """All :py:class:`transformlib.Output`(s) and :py:class:`transformlib.Input`(s)."""
        return [node for transform in self.transforms for node in transform.nodes]

    def run(self) -> None:
        """Used to run all the :py:class:`transformlib.Transform`(s) in the :py:class:`transformlib.Pipeline`."""
        logger.info(f"Beginning running of {self}")
        start = time.perf_counter()
        for transform in self.tasks:
            transform.run()
        logger.info(f"Completed running of {self} took {time.perf_counter() - start}")

    @classmethod
    def from_paths(cls, paths: list[str] | list[Path]):
        """Initialize a :py:class:`transformlib.Pipeline` from all :py:class:`transformlib.Transform`(s) found in a list of path(s) to modules."""
        transforms = []
        for path in list(map(Path, paths)):
            if path.suffix != ".py":
                raise NotImplementedError(f"path={path} is not a .py file")
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
