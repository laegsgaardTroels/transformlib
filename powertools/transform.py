from powertools import Node
from powertools import Output
from powertools import Input

from typing import List
from typing import Dict

import logging

logger = logging.getLogger(__name__)


class Transform:
    """Used to organize transformations of data."""

    def __init__(self, output_kwargs, func, input_kwargs):
        self.output_kwargs = output_kwargs
        self.func = func
        self.input_kwargs = input_kwargs

    @property
    def output_kwargs(self):
        return self._output_kwargs

    @output_kwargs.setter
    def output_kwargs(self, value):
        if len(set(value.values())) != len(value.values()):
            raise ValueError(f"Duplicate {value}")
        self._output_kwargs = value

    @property
    def input_kwargs(self):
        return self._input_kwargs

    @input_kwargs.setter
    def input_kwargs(self, value):
        if len(set(value.values())) != len(value.values()):
            raise ValueError(f"Duplicate {value}")
        self._input_kwargs = value

    @property
    def outputs(self) -> List[Output]:
        return tuple(self.output_kwargs.values())

    @property
    def inputs(self) -> List[Input]:
        return tuple(self.input_kwargs.values())

    @property
    def nodes(self) -> List[Node]:
        return self.outputs + self.inputs

    @property
    def kwargs(self) -> Dict[str, Node]:
        """The kwargs of a transform is the key value arguments inputted when running it.

        Having the nodes be identified by a key makes it clear what node is referred to
        in the function attribute (self.func). In below example it is clear what a, b
        refers to, it wouldn't be if one hadn't identified the input to the transform
        by a key value argument.

        >>> from powertools import transform
        >>> from pyspark.sql import functions as F
        >>> func = lambda a, b: a.write(b.read().agg(F.count('*')))
        >>> transform(a=Output('foo'), b=Input('bar'))(func)
        <class 'powertools.transform.Transform'>(func=<function <lambda> at 0x7f79251df4c0>, a=<class 'powertools.node.Output'>(path=/tmp/foo), b=<class 'powertools.node.Input'>(path=/tmp/bar))  # noqa

        Returns:
            Dict[Node]: The key value arguments to the function.
        """
        return {**self.output_kwargs, **self.input_kwargs}

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        kwargs_repr = ', '.join([
            f'{key}={repr(value)}'
            for key, value in self.kwargs.items()
        ])
        return f'{self.__class__}(func={repr(self.func)}, {kwargs_repr})'

    def __str__(self):
        return self.func.__name__

    def __eq__(self, other):
        if set(self.nodes) == set(other.nodes):
            return True
        return False

    def __hash__(self):
        return hash((self.func, *self.kwargs.items()))

    def _run(self, *param_args, **param_kwargs):
        return self(*param_args, **self.kwargs, **param_kwargs)

    def run(self, *param_args, **param_kwargs):
        """Runs a transform, the param_kwargs is overwritten by transform kwargs.

        A parameter to a transform is an external transform input which is
        NOT a DataFrame. This could be a model or the like.

        Args:
           *param_args List[Any]: A list of parameters to a transform.
           *param_kwargs (Dict[str, Any]): A list og keyword argument
                parameters to a transform.

        Returns:
            None: A transform run does not return anything but the side
                effects are specified by the output nodes in the transforms.
        """
        logger.info(f'BEGINNING RUNNING OF {self}.')
        res = self._run(*param_args, **param_kwargs)
        logger.info(f'COMPLETED RUNNING OF {self}.')
        return res


class DataFrameTransform(Transform):
    """Used to organize transformations with DataFrame input.

    The DataFrameTransform has only a single DataFrame output
    and multiple DataFrame inputs.
    """

    def __init__(self, output_kwargs, func, input_kwargs):
        super().__init__(output_kwargs, func, input_kwargs)

    @property
    def output(self):
        return self.output_kwargs['output']

    def _run(self, *param_args, **param_kwargs):
        """Data is loaded  AND only inputs are given as kwargs."""
        res = self(**{key: self.input_kwargs[key].load() for key in self.input_kwargs})
        self.output.save(res)


def transform(**kwargs: Node):
    """Convert a function to a Transform.

    Args:
        **kwargs (Dict[str, Node]): The inputs and outputs of the transform.

    Returns:
        function: A decorator that returns a Transform object.
    """
    def decorator(func) -> Transform:
        output_kwargs = _filter_outputs(kwargs)
        input_kwargs = _filter_inputs(kwargs)
        return Transform(output_kwargs, func, input_kwargs)
    return decorator


def transform_df(output: Output, **kwargs: Input):
    """Convert a function to a DataFrameTransform.

    Args:
        output (Output): The single output of the Transform.
        **input_kwargs (Dict[str, Input]): The inputs of the transform.

    Returns:
        function: A decorator that returns a DataFrameTransform object.
    """
    def decorator(func) -> DataFrameTransform:
        output_kwargs = {'output': output}
        input_kwargs = _filter_inputs(kwargs)
        return DataFrameTransform(output_kwargs, func, input_kwargs)
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
