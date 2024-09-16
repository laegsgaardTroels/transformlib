import pytest
from transformlib import (
    transform,
    Pipeline,
    Input,
    Output,
    TransformlibCycleException,
    TransformlibDuplicateTransformException,
)


def test_run_duplicate_transforms():
    """Should raise exception if duplicate Transform."""

    @transform()
    def empty():
        return None

    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([empty, empty])

    @transform(a=Output("a"))
    def out(a):
        return None

    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([out, out])

    @transform(b=Output("b"))
    def inp(b):
        return None

    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([inp, inp])


def test_run_transform_exception_handling():
    """Should raise an exception if one is raised in a Transform."""

    class TransformlibTestRunTasksException(Exception):
        """Raised in this test case."""

    @transform()
    def raise_exception():
        raise TransformlibTestRunTasksException("Transform test.")

    pipeline = Pipeline([raise_exception])
    with pytest.raises(TransformlibTestRunTasksException):
        pipeline.run()


def test_run_cycle_exception():
    """Should raise an exception if a cycle in the DAG."""

    @transform(
        a=Output("a"),
        b=Input("b"),
    )
    def ab(a, b):
        return None

    @transform(
        b=Output("b"),
        a=Input("a"),
    )
    def ba(a, b):
        return None

    pipeline = Pipeline([ab, ba])
    with pytest.raises(TransformlibCycleException):
        pipeline.run()
