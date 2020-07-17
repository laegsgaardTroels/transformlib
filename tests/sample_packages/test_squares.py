from powertools import ReusedPySparkTestCase

from squares.pipelines import pipeline


class TestSquares(ReusedPySparkTestCase):
    """Used to test that powertools works with the squares package."""

    def test_pipeline(self):
        """"Test that the pipeline runs."""
        pipeline.run()
