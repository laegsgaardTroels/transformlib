"""Contains the Transform abstraction and helper methods.
"""
from transformlib import Node
from transformlib import Output
from transformlib import Input

import time
from typing import Tuple
from typing import Dict

import logging

logger = logging.getLogger(__name__)


class Transform:
    """Used to organize transformations of data.

    A `Transform` is a many to many mapping between `Input` and `Output` nodes. When run the
    `Input` nodes are loaded and the calculated outputs from the function is written to the
    `Output` nodes.
    """

    def __init__(self, output_kwargs, func, input_kwargs):
        self.output_kwargs = output_kwargs
        self.func = func
        self.input_kwargs = input_kwargs

    @property
    def outputs(self) -> Tuple[Output]:
        """A tuple with all the Outputs of the Transform.
        """
        return tuple(self.output_kwargs.values())

    @property
    def inputs(self) -> Tuple[Input]:
        """A tuple with all the Inputs of the Transform.
        """
        return tuple(self.input_kwargs.values())

    @property
    def nodes(self) -> Tuple[Node]:
        """A tuple with all the Nodes of the Transform e.g. Input and Output.
        """
        return tuple(self.outputs + self.inputs)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
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

    def __str__(self):
        return self.func.__name__

    def __eq__(self, other):
        if set(self.nodes) == set(other.nodes):
            return True
        return False

    def __hash__(self):
        return hash((self.func, *self.output_kwargs.items(), *self.input_kwargs.items()))

    def run(self) -> None:
        """Runs a transform."""
        logger.info(f'Beginning running of {self}.')
        start = time.perf_counter()
        self(**self.output_kwargs, **self.input_kwargs)
        logger.info(f'Completed running of {self} took {time.perf_counter() - start}.')


def transform(**kwargs: Node):
    """Convert a function to a Transform.

    Args:
        **kwargs (Dict[str, Node]): The nodes of the transform.

    Returns:
        function: A decorator that returns a Transform object.
    """
    def decorator(func) -> Transform:
        output_kwargs = _filter_outputs(kwargs)
        input_kwargs = _filter_inputs(kwargs)
        return Transform(output_kwargs, func, input_kwargs)
    return decorator


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
