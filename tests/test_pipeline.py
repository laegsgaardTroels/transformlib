from unittest.mock import patch

from powertools import ReusedPySparkTestCase
from powertools import Transform
from powertools import Pipeline
from powertools.pipeline import _tsort
from powertools import transform_df
from powertools import Output
from powertools import Input

from pyspark.sql import functions as F


class TestPipeline(ReusedPySparkTestCase):
    """Used to test the Pipeline class."""

    def test_run_tasks_duplicate_testing(self):
        """Should raise error if duplicate transform in TESTING.

        It is assumed that the environment is TESTING when running
        the unittest.
        """
        transform = Transform(
            output_kwargs={},
            func=lambda: None,
            input_kwargs={},
        )
        with self.assertRaises(ValueError):
            pipeline = Pipeline([transform, transform])
            pipeline.run()

    def test_run_tasks_exception_testing(self):
        """Should raise an exception in TESTING.

        It is assumed that the environment is TESTING when running
        the unittest.
        """

        def raise_transform_exception():
            raise Exception('Transform test.')

        transform = Transform(
            output_kwargs={},
            func=raise_transform_exception,
            input_kwargs={},
        )

        with self.assertRaises(Exception):
            pipeline = Pipeline([transform])
            pipeline.run()

    @patch('powertools.config.ENVIRONMENT', 'PRODUCTION')
    def test_run_tasks_duplicate_production(self):
        """Should NOT raise error if duplicate transform in PRODUCTION.

        If there is duplicate Transforms in the Pipeline then the Transform will
        only be run once in PRODUCTION.
        """
        transform = Transform(
            output_kwargs={},
            func=lambda: None,
            input_kwargs={},
        )
        try:
            pipeline = Pipeline([transform, transform])
            metadata = pipeline.run()
        except Exception:
            self.fail("Should not raise an exception in production.")
        assert len(metadata) == 1, (
            f"Should have a row for each transform {metadata}."
        )

    @patch('powertools.config.ENVIRONMENT', 'PRODUCTION')
    def test_run_tasks_exception_production(self):
        """Should NOT raise an exception in PRODUCTION."""

        def raise_transform_exception():
            raise Exception('Transform test.')

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

    def test_sample_square(self):
        """A sample usecase of the pipeline from the README."""

        @transform_df(Output('range.parquet'))
        def range():
            return self.spark.range(100)

        @transform_df(
            Output('squares.parquet'),
            range=Input('range.parquet'),
        )
        def square(range):
            return (
                range
                .withColumn(
                    'squares',
                    F.pow(F.col('id'), F.lit(2))
                )
            )
        pipeline = Pipeline([range, square])
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
