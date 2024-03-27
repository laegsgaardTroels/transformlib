import pytest
from transformlib import (
    Transform,
    Output,
    Input,
    TransformlibDuplicateOutputException,
    TransformlibDuplicateInputException,
)


def test_transform_call():
    """Test the __call__() method of the Transform class."""
    transform = Transform(
        output_kwargs={},
        function=lambda a, b: a + b,
        input_kwargs={},
    )
    assert transform(1, 1) == 2, "Something went wrong in Transform.__call__."


def test_transform_set():
    """Test that running set() on a list of transforms removes duplicates."""
    transform = Transform(
        output_kwargs={},
        function=lambda: None,
        input_kwargs={},
    )
    assert len(set([transform, transform])) == 1


def test_duplicate_output_exception():
    with pytest.raises(TransformlibDuplicateOutputException):
        Transform(
            output_kwargs={"foo_output0": Output("foo"), "foo_output1": Output("foo")},
            function=lambda foo_output: None,
            input_kwargs={},
        )


def test_duplicate_input_exception():
    with pytest.raises(TransformlibDuplicateInputException):
        Transform(
            output_kwargs={},
            function=lambda foo_output: None,
            input_kwargs={"foo_input0": Input("foo"), "foo_input1": Input("foo")},
        )
