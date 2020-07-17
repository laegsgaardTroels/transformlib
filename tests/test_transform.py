from powertools import ReusedPySparkTestCase
from powertools import Transform


class TestTransform(ReusedPySparkTestCase):
    """Used to test the Transform class."""

    def test_transform_call(self):
        """Test the __call__() method of the Transform class."""
        transform = Transform(
            output_kwargs={},
            func=lambda a, b: a + b,
            input_kwargs={},
        )
        assert transform(1, 1) == 2, (
            "Something went wrong in Transform.__call__."
        )

    def test_transform_run(self):
        """Test the .run() method of the Transform class."""
        transform = Transform(
            output_kwargs={},
            func=lambda a, b: a + b,
            input_kwargs={'b': 1},
        )
        assert transform.run(1) == 2, "Parameters not correct."

        with self.assertRaises(TypeError):
            # Parameters cannot overwrite transform kwargs.
            # So this should raise an error.
            transform.run(1, 2)

        with self.assertRaises(TypeError):
            # Parameters cannot overwrite transform kwargs.
            # So this should raise an error.
            transform.run(1, b=2)

    def test_transform_set(self):
        """Test that running set() on a list of transforms removes duplicates."""
        transform = Transform(
            output_kwargs={},
            func=lambda: None,
            input_kwargs={},
        )
        assert len(set([transform, transform])) == 1
