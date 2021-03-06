from transformlib.testing import ReusedPySparkTestCase
from transformlib import Transform
from transformlib import Pipeline
from transformlib.exceptions import TransformlibDuplicateTransformException
from transformlib.pipeline import _tsort


class TestPipeline(ReusedPySparkTestCase):
    """Used to test the Pipeline class."""

    def test_run_tasks_duplicate_testing(self):
        """Should raise exception if duplicate Transform."""
        transform = Transform(
            output_args=[],
            func=lambda: None,
            input_kwargs={},
        )
        with self.assertRaises(TransformlibDuplicateTransformException):
            pipeline = Pipeline([transform, transform])
            pipeline.run()

    def test_run_tasks_exception_testing(self):
        """Should raise an exception if one is raised in a Transform."""
        class TransformlibTestRunTasksException(Exception):
            """Raised in this test case."""

        def raise_transform_exception():
            raise TransformlibTestRunTasksException('Transform test.')

        transform = Transform(
            output_args=[],
            func=raise_transform_exception,
            input_kwargs={},
        )

        with self.assertRaises(TransformlibTestRunTasksException):
            pipeline = Pipeline([transform])
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
