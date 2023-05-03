from transformlib import Node
from transformlib import Output
from transformlib import Input

import time
from typing import Tuple
from typing import Dict

import logging

logger = logging.getLogger(__name__)


def transform(**kwargs: Node) -> 'Transform':
    """Convert a function to a :py:class:`transformlib.Transform`.

    Args:
        **kwargs (Dict[str, Node]): The :py:class:`transformlib.Input`
            and :py:class:`transformlib.Output` nodes of the transform.

    Returns:
        function: A decorator that returns a Transform object.
    """
    def decorator(func) -> Transform:
        output_kwargs = _filter_outputs(kwargs)
        input_kwargs = _filter_inputs(kwargs)
        return Transform(output_kwargs, func, input_kwargs)
    return decorator


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

    def __init__(self, output_kwargs, func, input_kwargs):
        self.output_kwargs = output_kwargs
        self.func = func
        self.input_kwargs = input_kwargs

    @property
    def outputs(self) -> Tuple[Output]:
        """A tuple with all the :py:class:`transformlib.Output` of the Transform."""
        return tuple(self.output_kwargs.values())

    @property
    def inputs(self) -> Tuple[Input]:
        """A tuple with all the :py:class:`transformlib.Input` to the Transform."""
        return tuple(self.input_kwargs.values())

    @property
    def nodes(self) -> Tuple[Node]:
        """All :py:class:`transformlib.Output` and :py:class:`transformlib.Input`."""
        return self.outputs + self.inputs

    def run(self) -> None:
        """Runs the Transform."""
        logger.info(f'Beginning running of {self}.')
        start = time.perf_counter()
        self(**self.output_kwargs, **self.input_kwargs)
        logger.info(f'Completed running of {self} took {time.perf_counter() - start}.')

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}('
            + ', '.join(map(
                lambda key: key + '=' + repr(self.output_kwargs[key]),
                self.output_kwargs
            ))
            + (', ' if self.input_kwargs else '')
            + ', '.join(map(
                lambda key: key + '=' + repr(self.input_kwargs[key]),
                self.input_kwargs
            ))
            + ')'
        )

    def __str__(self) -> str:
        return self.func.__name__

    def __eq__(self, other) -> bool:
        if set(self.nodes) == set(other.nodes):
            return True
        return False

    def __hash__(self) -> int:
        return hash(self.nodes)


def _filter_outputs(kwargs) -> Dict[str, Output]:
    """Filter out anything that is not an Output."""
    return {
        key: kwargs[key] for key in kwargs
        if isinstance(kwargs[key], Output)
    }


def _filter_inputs(kwargs) -> Dict[str, Input]:
    """Filter out anything that is not an Input."""
    return {
        key: kwargs[key] for key in kwargs
        if isinstance(kwargs[key], Input)
    }
