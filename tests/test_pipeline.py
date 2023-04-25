import pytest

from tests.utils import ReusedPySparkTestCase
from transformlib import (
    Transform,
    Pipeline
)
from transformlib.exceptions import (
    TransformlibDuplicateTransformException,
    TransformlibCycleException
)
from transformlib.pipeline import _tsort


def test_run_tasks_duplicate_testing():
    """Should raise exception if duplicate Transform."""
    transform = Transform(
        output_kwargs={},
        func=lambda: None,
        input_kwargs={},
    )
    with pytest.raises(TransformlibDuplicateTransformException):
        pipeline = Pipeline([transform, transform])
        pipeline.run()


def test_run_tasks_exception_testing():
    """Should raise an exception if one is raised in a Transform."""
    class TransformlibTestRunTasksException(Exception):
        """Raised in this test case."""

    def raise_transform_exception():
        raise TransformlibTestRunTasksException('Transform test.')

    transform = Transform(
        output_kwargs={},
        func=raise_transform_exception,
        input_kwargs={},
    )

    with pytest.raises(TransformlibTestRunTasksException):
        pipeline = Pipeline([transform])
        pipeline.run()


class TestSquares(ReusedPySparkTestCase):
    """Used to test that transformlib on the squares transforms."""

    def test_squares_discover_transforms(self):
        """"Test that the pipeline runs."""
        from tests.transforms import squares
        pipeline = Pipeline.discover_transforms(squares)
        pipeline.run()

    def test_squares_with_cycle_discover_transforms(self):
        """"Test that the pipeline runs and raises an Exception."""
        from tests.transforms import squares_with_cycle
        pipeline = Pipeline.discover_transforms(squares_with_cycle)
        with pytest.raises(TransformlibCycleException):
            pipeline.run()


def test_tsort():
    """Used to test the topological sort."""
    graph_tasks = {
        "wash the dishes": ["have lunch"],
        "cook food": ["have lunch"],
        "have lunch": [],
        "wash laundry": ["dry laundry"],
        "dry laundry": ["fold laundry"],
        "fold laundry": []
    }
    order = _tsort(graph_tasks)
    for idx, task in enumerate(order):
        assert all(todo in order[idx:] for todo in graph_tasks[task]), (
            f"Missing todo after {task}, todos: {graph_tasks[task]} "
            f"in order: {order}."
        )
