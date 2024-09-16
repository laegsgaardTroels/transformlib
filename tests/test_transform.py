import pytest
from transformlib import (
    transform,
    Output,
    Input,
    TransformlibDuplicateOutputException,
    TransformlibDuplicateInputException,
)


def test_transform_call():
    """Test the __call__() method of the Transform class."""

    @transform()
    def add(a, b):
        return a + b

    assert add(1, 1) == 2, "Something went wrong in Transform.__call__."


def test_transform_set():
    """Test that running set() on a list of transforms removes duplicates."""

    @transform()
    def empty():
        return None

    assert len(set([empty, empty])) == 1


def test_duplicate_output_exception():
    with pytest.raises(TransformlibDuplicateOutputException):

        @transform(
            foo_output0=Output("foo"),
            foo_output1=Output("foo"),
        )
        def duplicate():
            return None


def test_duplicate_input_exception():
    with pytest.raises(TransformlibDuplicateInputException):

        @transform(
            foo_output0=Input("foo"),
            foo_output1=Input("foo"),
        )
        def duplicate():
            return None
