from pathlib import Path
import graphlib
import importlib
import sys
import time
import typing
import functools
import logging

logger = logging.getLogger(__name__)


class TransformlibCycleException(graphlib.CycleError):
    """Raised when there is a cycle in the :py:class:`~transformlib.Pipeline`."""


class TransformlibDuplicateTransformException(Exception):
    """Raised when there is duplicate Transform in the :py:class:`~transformlib.Pipeline`."""


class TransformlibDuplicateInputException(Exception):
    """Raised when there is duplicate Input in the :py:class:`~transformlib.Pipeline`."""


class TransformlibDuplicateOutputException(Exception):
    """Raised when there is duplicate Output in the :py:class:`~transformlib.Pipeline`."""


class TransformlibSettings:
    """Settings used by transformlib."""

    _config = {"data_dir": "/tmp/"}

    def configure(self, data_dir: str) -> None:
        self.data_dir = data_dir

    @property
    def data_dir(self) -> Path:
        """The root directory where all data is saved and loaded relative to."""
        return Path(self._config["data_dir"])

    @data_dir.setter
    def data_dir(self, value: str) -> None:
        self._config["data_dir"] = value


def configure(data_dir: str) -> None:
    TransformlibSettings().configure(data_dir=data_dir)


# Automate creation and re-use of docstring for TransformlibSettings.configure() and configure()
TransformlibSettings.configure.__doc__ = """Configure settings.

Args:
""" + "\n".join(
    [
        f"   {name}: {getattr(TransformlibSettings, name).__doc__}"
        for name in TransformlibSettings._config
    ]
)
configure.__doc__ = TransformlibSettings.configure.__doc__


class Reader(typing.Protocol):
    """Used to read data from disk."""

    def __call__(self, path: Path, **metadata: typing.Any) -> typing.Any:
        ...


class Writer(typing.Protocol):
    """Used to write data to disk."""

    def __call__(self, obj: typing.Any, path: Path, **metadata: typing.Any) -> None:
        ...


class Node:
    """The Node base class is a node in a directed asyclic graph of data transformations.

    Args:
        relative_path (Path | str): The path relative to :py:class:`~transformlib.TransformlibSettings.data_dir` where
            data associated with the node is saved or loaded from.
        reader (Reader | None): An optional reader used to read data from the node.
        writer (Writer | None): An optional writer used to write data to the node.
        **metadata (dict[str, Any]): A dictionary with metadata associated with the node.

    Attributes:
        relative_path (Path | str): The path relative to :py:class:`~transformlib.TransformlibSettings.data_dir` where
            data associated with the node is saved or loaded from.
        reader (Reader | None): An optional reader used to read data from the node.
        writer (Writer | None): An optional writer used to write data to the node.
        metadata (dict[str, Any]): A dictionary with metadata associated with the node.
    """

    def __init__(
        self,
        relative_path: Path | str,
        reader: Reader | None = None,
        writer: Writer | None = None,
        **metadata: typing.Any,
    ):
        self.relative_path = relative_path
        self.reader = reader
        self.writer = writer
        self.metadata = metadata

    @property
    def path(self) -> Path:
        """The path to the node in the directory :py:class:`~transformlib.TransformlibSettings.data_dir`."""
        return TransformlibSettings().data_dir / self.relative_path

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.relative_path})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Node):
            return False
        return self.path == other.path

    def __hash__(self) -> int:
        return hash(self.path)


class Output(Node):
    """An Output is a sink in a directed asyclic graph of data transformations."""


class Input(Node):
    """An Input is a source in a directed asyclic graph of data transformations."""


class Parameter:
    """A Parameter can be used to parameterize a Node in a directed asyclic graph of data transformations.

    Args:
        value (str | int | float | complex | bool | None): The current value of the Parameter.
    """

    def __init__(self, value: str | int | float | complex | bool | None):
        self.value = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)


Function = typing.Callable
"""A function that contains the data transformation logic used to load and
transform the :py:class:`~transformlib.Input` nodes and save the output to the
:py:class:`~transformlib.Output` nodes."""


class Transform:
    """A :py:class:`~transformlib.Transform` loads, transforms and saves data.

    A :py:class:`~transformlib.Transform` is a lazily evaluated many to many transformation
    between :py:class:`~transformlib.Input` and :py:class:`~transformlib.Output` nodes.

    Args:
        function (Function): A function that contains the data transformation logic used to load and
            transform the :py:class:`~transformlib.Input` nodes and save the output to the
            :py:class:`~transformlib.Output` nodes.
        args (tuple[Input | Output | Parameter, ...]):  All arguments to the transformation.
        kwargs (dict[str, Input | Output | Parameter] | dict[str, Input | Parameter]): All
            keyword arguments to the transformation.

    Raises:
        TransformlibDuplicateInputException: If duplicate :py:class:`~transformlib.Input` exists.
        TransformlibDuplicateOutputException: If duplicate :py:class:`~transformlib.Output` exists.
    """

    def __init__(
        self,
        function: Function,
        args: tuple[Input | Output | Parameter, ...],
        kwargs: dict[str, Input | Output | Parameter] | dict[str, Input | Parameter],
    ):
        self.function = function
        self.args = args
        self.kwargs = kwargs

        if len(set(self.inputs)) != len(self.inputs):
            raise TransformlibDuplicateInputException(
                f"Duplicate inputs={self.inputs}")
        if len(set(self.outputs)) != len(self.outputs):
            raise TransformlibDuplicateOutputException(
                f"Duplicate outputs={self.outputs}"
            )

    @property
    def output_args(self) -> tuple[Output, ...]:
        """A tuple with all the :py:class:`~transformlib.Output` arguments."""
        return tuple([value for value in self.args if isinstance(value, Output)])

    @property
    def input_args(self) -> tuple[Input, ...]:
        """A tuple with all the :py:class:`~transformlib.Input` arguments."""
        return tuple([value for value in self.args if isinstance(value, Input)])

    @property
    def parameter_args(self) -> tuple[Parameter, ...]:
        """A tuple with all the :py:class:`~transformlib.Parameter` arguments."""
        return tuple([value for value in self.args if isinstance(value, Parameter)])

    @property
    def output_kwargs(self) -> dict[str, Output]:
        """A dictionary with all the :py:class:`~transformlib.Output` keyword arguments."""
        return {
            key: value
            for key, value in self.kwargs.items()
            if isinstance(value, Output)
        }

    @property
    def input_kwargs(self) -> dict[str, Input]:
        """A dictionary with all the :py:class:`~transformlib.Input` keyword arguments."""
        return {
            key: value for key, value in self.kwargs.items() if isinstance(value, Input)
        }

    @property
    def parameter_kwargs(self) -> dict[str, Parameter]:
        """A dictionary with all the :py:class:`~transformlib.Parameter` keyword arguments."""
        return {
            key: value
            for key, value in self.kwargs.items()
            if isinstance(value, Parameter)
        }

    @property
    def outputs(self) -> tuple[Output, ...]:
        """A tuple with all the :py:class:`~transformlib.Output`\\ (s) of the :py:class:`~transformlib.Transform`."""
        return self.output_args + tuple(self.output_kwargs.values())

    @property
    def inputs(self) -> tuple[Input, ...]:
        """A tuple with all the :py:class:`~transformlib.Input`\\ (s) to the :py:class:`~transformlib.Transform`."""
        return self.input_args + tuple(self.input_kwargs.values())

    @property
    def nodes(self) -> tuple[Input | Output, ...]:
        """All :py:class:`~transformlib.Output`\\ (s) and :py:class:`~transformlib.Input`\\ (s)."""
        return self.outputs + self.inputs

    @property
    def parameters(self) -> tuple[Parameter, ...]:
        """All :py:class:`~transformlib.Parameter`\\ (s)."""
        return self.parameter_args + tuple(self.parameter_kwargs.values())

    def run(self) -> None:
        """Loads data from the :py:class:`~transformlib.Input`\\ (s), transforms it and saves data to the :py:class:`~transformlib.Output`\\ (s)."""
        logger.info(f"Beginning running of {self}")
        start = time.perf_counter()
        self.function(*self.args, **self.kwargs)
        logger.info(
            f"Completed running of {self} took {time.perf_counter() - start}")

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + _args_repr(self.args)
            + ", "
            + _kwargs_repr(self.kwargs)
            + ")"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, Transform):
            return False
        if set(self.nodes) == set(other.nodes):
            return True
        return False

    def __hash__(self) -> int:
        return hash(tuple(self.nodes))

    def __name__(self) -> str:
        return self.function.__name__


def _args_repr(args: tuple[typing.Any, ...]) -> str:
    return ", ".join(map(str, args))


def _kwargs_repr(kwargs: dict[str, typing.Any]) -> str:
    return ", ".join(
        map(
            lambda key: key + "=" + repr(kwargs[key]),
            kwargs,
        )
    )


def transform(
    *args: Input | Output | Parameter,
    **kwargs: Input | Output | Parameter,
) -> typing.Callable[[Function], Transform]:
    """Convert a function to a :py:class:`~transformlib.Transform`.

    A Transform is often constructed using the :py:func:`~transformlib.transform` decorator:

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

    In above example the ``convert_to_json`` is a :py:class:`~transformlib.Transform` object that
    can be part of a :py:class:`~transformlib.Pipeline` of many transformations.

    Args:
        *args (Input | Output | Parameter):  All arguments to the transformation.
        **kwargs (Input | Output | Parameter): All keyword arguments to the transformation.

    Returns:
        Callable[[Function], Transform]: A decorator that returns a Transform object.
    """

    def decorator(function: Function) -> Transform:
        return Transform(function=function, args=args, kwargs=kwargs)

    return decorator


def transform_read_write(
    *args: Output,
    **kwargs: Input | Parameter,
) -> typing.Callable[[Function], Transform]:
    """Convert a function to a :py:class:`~transformlib.Transform`.

    Adds logic logic to run all reader(s) and forwarding read objects to the function
    and run all writer(s) and write the output of the function using these.

    Args:
        *args (Output): The :py:class:`~transformlib.Output`\\ (s) of the transform.
        **kwargs (Input | Parameter): The :py:class:`~transformlib.Input`\\ (s)
            and :py:class:`~transformlib.Parameter`\\ (s) of the transform.

    Returns:
        Callable[[Function], Transform]: A decorator that returns a Transform object.
    """

    def decorator(function: Function) -> Transform:
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            # Read Input(s)
            processed_kwargs = {}
            for key, value in kwargs.items():
                if isinstance(value, Input) and value.reader is not None:
                    processed_kwargs[key] = value.reader(
                        value.path, **value.metadata)
            # Transform
            output_objects = function(**processed_kwargs)

            # Save Output(s)
            if not isinstance(output_objects, tuple):
                output_objects = (output_objects,)
            try:
                for obj, output in zip(output_objects, args, strict=True):
                    output.writer(obj, output.path, **output.metadata)

            except Exception as exception:
                raise Exception("Unable to save outputs") from exception

        return Transform(function=wrapper, args=args, kwargs=kwargs)

    return decorator


class Pipeline:
    """A Pipeline is a topologically ordered list of :py:class:`~transformlib.Transform` objects.

    A :py:class:`~transformlib.Pipeline` can be run from the command line with the installed cli:

    .. highlight:: bash
    .. code-block:: bash

        ~$ transform path/to/transforms/*.py
        ~$ transform -v path/to/transforms/*.py

    This will topologically sort and run all :py:class:`~transformlib.Transform` objects found in
    the .py files.
    """

    def __init__(
        self, transforms: list[Transform] | dict[str, Transform] | None = None
    ):
        if transforms is None:
            self.transforms = {}
        elif isinstance(transforms, list):
            if len(set(transforms)) != len(transforms):
                raise TransformlibDuplicateTransformException(
                    f"Duplicate transforms={transforms}"
                )
            self.transforms = {t.__name__: t for t in transforms}
        elif isinstance(transforms, dict):
            self.transforms = transforms
        else:
            raise NotImplementedError(f"Not supported {transforms=}")

    @property
    def tasks(self) -> tuple[Transform, ...]:
        """A topologically sorted list of :py:class:`~transformlib.Transform`\\ (s) in the Pipeline."""
        tsort = graphlib.TopologicalSorter()
        for transform in self.transforms.values():
            predecessors = [
                t
                for t in self.transforms.values()
                if any(o in transform.inputs for o in t.outputs)
            ]
            tsort.add(transform, *predecessors)
        try:
            return tuple(tsort.static_order())
        except graphlib.CycleError as exception:
            raise TransformlibCycleException("Cycle detected.") from exception

    @property
    def outputs(self) -> tuple[Output, ...]:
        """A tuple with all the :py:class:`~transformlib.Output` of the :py:class:`~transformlib.Pipeline`."""
        return tuple(
            [
                node
                for transform in self.transforms.values()
                for node in transform.outputs
            ]
        )

    @property
    def inputs(self) -> tuple[Input, ...]:
        """A tuple with all the :py:class:`~transformlib.Input` of the :py:class:`~transformlib.Pipeline`."""
        return tuple(
            [
                node
                for transform in self.transforms.values()
                for node in transform.inputs
            ]
        )

    @property
    def nodes(self) -> tuple[Input | Output, ...]:
        """All :py:class:`~transformlib.Output`\\ (s) and :py:class:`~transformlib.Input`\\ (s)."""
        return tuple(
            [node for transform in self.transforms.values()
             for node in transform.nodes]
        )

    @property
    def parameters(self) -> tuple[Parameter, ...]:
        """All :py:class:`~transformlib.Parameter`\\ (s)."""
        return tuple(
            [
                parameter
                for transform in self.transforms.values()
                for parameter in transform.parameters
            ]
        )

    def run(self) -> None:
        """Used to run all the :py:class:`~transformlib.Transform`\\ (s) in the :py:class:`~transformlib.Pipeline`."""
        logger.info(f"Beginning running of {self}")
        start = time.perf_counter()
        for transform in self.tasks:
            transform.run()
        logger.info(
            f"Completed running of {self} took {time.perf_counter() - start}")

    @classmethod
    def from_paths(cls, paths: list[str] | list[Path]):
        """Initialize a :py:class:`~transformlib.Pipeline` from all :py:class:`~transformlib.Transform`\\ (s) found in a list of path(s) to .py files.

        As part of this initialization the parent folder to each path is appended to ``PYTHONPATH``.
        """
        transforms = {}
        for path in list(map(Path, paths)):
            if path.stem.startswith("__"):
                continue
            module = import_and_append_to_sys_path(path)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, Transform):
                    logger.info(f"Discovered Transform: {attr} in {path}")
                    transforms[f"{path.resolve()}::{attr.__name__}"] = attr
        return cls(transforms)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(" + _args_repr(tuple(self.transforms)) + ")"

    def __len__(self) -> int:
        return len(self.transforms)


def import_and_append_to_sys_path(path: Path):
    """Import a .py file as a module and append its folder to PYTHONPATH."""
    if path.suffix != ".py":
        raise NotImplementedError(f"path={path} is not a .py file")
    if str(path.parent.resolve()) not in sys.path:
        sys.path.append(str(path.parent.resolve()))
    return importlib.import_module(path.stem)
