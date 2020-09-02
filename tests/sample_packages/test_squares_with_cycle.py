from powertools.testing import ReusedPySparkTestCase
from powertools.exceptions import PowertoolsCycleException

from squares_with_cycle.pipelines import pipeline


class TestSquaresWithDag(ReusedPySparkTestCase):
    """Used to test that powertools works with the squares_with_dag package."""

    def test_pipeline(self):
        """"Test that the pipeline runs."""
        with self.assertRaises(PowertoolsCycleException):
            pipeline.run()
