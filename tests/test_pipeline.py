import pytest
from transformlib import (
    Transform,
    Pipeline,
    Input,
    Output,
    TransformlibCycleException,
    TransformlibDuplicateTransformException,
)


def test_run_duplicate_transforms():
    """Should raise exception if duplicate Transform."""
    transform = Transform(
        output_kwargs={},
        function=lambda: None,
        input_kwargs={},
    )
    with pytest.raises(Exception):
        pipeline = Pipeline([transform, transform])
        pipeline.run()


def test_run_transform_exception_handling():
    """Should raise an exception if one is raised in a Transform."""

    class TransformlibTestRunTasksException(Exception):
        """Raised in this test case."""

    def raise_transform_exception():
        raise TransformlibTestRunTasksException("Transform test.")

    transform = Transform(
        output_kwargs={},
        function=raise_transform_exception,
        input_kwargs={},
    )
    pipeline = Pipeline([transform])
    with pytest.raises(TransformlibTestRunTasksException):
        pipeline.run()


def test_run_cycle_exception():
    """Should raise an exception if a cycle in the DAG."""

    def function1(foo_input, bar_input):
        return None

    def function2(bar_input, foo_input):
        return None

    transform1 = Transform(
        output_kwargs={"foo_input": Output("foo")},
        function=function1,
        input_kwargs={"bar_input": Input("bar")},
    )
    transform2 = Transform(
        output_kwargs={"bar_output": Output("bar")},
        function=function2,
        input_kwargs={"foo_input": Input("foo")},
    )
    pipeline = Pipeline([transform1, transform2])
    with pytest.raises(TransformlibCycleException):
        pipeline.run()


def test_duplicate_transform_exception():
    transform1 = Transform(
        output_kwargs={"foo_output": Output("foo")},
        function=lambda foo_output: None,
        input_kwargs={},
    )
    transform2 = Transform(
        output_kwargs={"foo_output": Output("foo")},
        function=lambda foo_output: None,
        input_kwargs={},
    )
    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([transform1, transform2])

    transform1 = Transform(
        output_kwargs={},
        function=lambda foo_output: None,
        input_kwargs={"foo_input": Input("foo")},
    )
    transform2 = Transform(
        output_kwargs={},
        function=lambda foo_output: None,
        input_kwargs={"foo_input": Input("foo")},
    )
    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([transform1, transform2])
