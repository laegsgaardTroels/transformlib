from unittest.mock import patch

from powertools import ReusedPySparkTestCase
from powertools import Transform
from powertools import Pipeline
from powertools.exceptions import PowertoolsDuplicateTransformException
from powertools.pipeline import _tsort


class TestPipeline(ReusedPySparkTestCase):
    """Used to test the Pipeline class."""

    def test_run_tasks_duplicate_testing(self):
        """Should raise error if duplicate transform in TESTING.

        It is assumed that the environment is TESTING when running
        the unittest. When TESTING an exception should be raised
        when a Pipeline is run.
        """
        transform = Transform(
            output_kwargs={},
            func=lambda: None,
            input_kwargs={},
        )
        with self.assertRaises(PowertoolsDuplicateTransformException):
            pipeline = Pipeline([transform, transform])
            pipeline.run()

    def test_run_tasks_exception_testing(self):
        """Should raise an exception in TESTING.

        It is assumed that the environment is TESTING when running
        the unittest. When TESTING an exception should be raised
        when a Pipeline is run.
        """
        class PowertoolsTestRunTasksException(Exception):
            """Raised in this test case."""

        def raise_transform_exception():
            raise PowertoolsTestRunTasksException('Transform test.')

        transform = Transform(
            output_kwargs={},
            func=raise_transform_exception,
            input_kwargs={},
        )

        with self.assertRaises(PowertoolsTestRunTasksException):
            pipeline = Pipeline([transform])
            pipeline.run()

    @patch('powertools.config.ENVIRONMENT', 'PRODUCTION')
    def test_run_tasks_duplicate_production(self):
        """Should raise error if duplicate transform in PRODUCTION.

        When PRODUCTION an exception should be raised when a Pipeline is run.
        """
        transform = Transform(
            output_kwargs={},
            func=lambda: None,
            input_kwargs={},
        )
        with self.assertRaises(PowertoolsDuplicateTransformException):
            pipeline = Pipeline([transform, transform])
            pipeline.run()

    @patch('powertools.config.ENVIRONMENT', 'PRODUCTION')
    def test_run_tasks_exception_production(self):
        """Should NOT raise an exception in PRODUCTION.

        When running the tasks in production a single failed transform
        should not make an entire pipeline fail.
        """
        class PowertoolsTestRunTasksProdException(Exception):
            """Raised in this test case."""

        def raise_transform_exception():
            raise PowertoolsTestRunTasksProdException('Transform test.')

        transform = Transform(
            output_kwargs={},
            func=raise_transform_exception,
            input_kwargs={},
        )

        try:
            pipeline = Pipeline([transform])
            metadata = pipeline.run()
        except Exception:
            self.fail("Should not raise an exception in production.")
        assert len(metadata) == 1, (
            f"Should have a row for each transform {metadata}."
        )


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
