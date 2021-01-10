from transformlib.testing import ReusedPySparkTestCase
from transformlib.exceptions import TransformlibCycleException

from squares_with_cycle.pipelines import pipeline


class TestSquaresWithDag(ReusedPySparkTestCase):
    """Used to test that powertools works with the squares_with_dag package."""

    def test_pipeline(self):
        """"Test that the pipeline runs."""
        with self.assertRaises(TransformlibCycleException):
            pipeline.run()
