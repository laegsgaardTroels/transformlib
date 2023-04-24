from transformlib import Transform


def test_transform_call():
    """Test the __call__() method of the Transform class."""
    transform = Transform(
        output_kwargs={},
        func=lambda a, b: a + b,
        input_kwargs={},
    )
    assert transform(1, 1) == 2, (
        "Something went wrong in Transform.__call__."
    )


def test_transform_set():
    """Test that running set() on a list of transforms removes duplicates."""
    transform = Transform(
        output_kwargs={},
        func=lambda: None,
        input_kwargs={},
    )
    assert len(set([transform, transform])) == 1
