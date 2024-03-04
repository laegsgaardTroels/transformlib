import pytest
from transformlib import (
    config,
    Transform,
    Pipeline,
    Input,
    Output,
    TransformlibCycleException,
    TransformlibDuplicateTransformException,
)


def test_run_duplicate_transforms():
    """Should raise exception if duplicate Transform."""
    transform = Transform(
        output_kwargs={},
        func=lambda: None,
        input_kwargs={},
    )
    with pytest.raises(Exception):
        pipeline = Pipeline([transform, transform])
        pipeline.run()


def test_run_transform_exception_handling():
    """Should raise an exception if one is raised in a Transform."""

    class TransformlibTestRunTasksException(Exception):
        """Raised in this test case."""

    def raise_transform_exception():
        raise TransformlibTestRunTasksException("Transform test.")

    transform = Transform(
        output_kwargs={},
        func=raise_transform_exception,
        input_kwargs={},
    )
    pipeline = Pipeline([transform])
    with pytest.raises(TransformlibTestRunTasksException):
        pipeline.run()


def test_run_cycle_exception():
    """Should raise an exception if a cycle in the DAG."""

    def func1(foo_input, bar_input):
        return None

    def func2(bar_input, foo_input):
        return None

    transform1 = Transform(
        output_kwargs={"foo_input": Output("foo")},
        func=func1,
        input_kwargs={"bar_input": Input("bar")},
    )
    transform2 = Transform(
        output_kwargs={"bar_output": Output("bar")},
        func=func2,
        input_kwargs={"foo_input": Input("foo")},
    )
    pipeline = Pipeline([transform1, transform2])
    with pytest.raises(TransformlibCycleException):
        pipeline.run()


def test_duplicate_transform_exception():
    transform1 = Transform(
        output_kwargs={"foo_output": Output("foo")},
        func=lambda foo_output: None,
        input_kwargs={},
    )
    transform2 = Transform(
        output_kwargs={"foo_output": Output("foo")},
        func=lambda foo_output: None,
        input_kwargs={},
    )
    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([transform1, transform2])

    transform1 = Transform(
        output_kwargs={},
        func=lambda foo_output: None,
        input_kwargs={"foo_input": Input("foo")},
    )
    transform2 = Transform(
        output_kwargs={},
        func=lambda foo_output: None,
        input_kwargs={"foo_input": Input("foo")},
    )
    with pytest.raises(TransformlibDuplicateTransformException):
        Pipeline([transform1, transform2])


def test_discover_transforms(tmp_path, monkeypatch, mapping_txt, mapping_py):
    """ "Test that the pipeline runs."""
    monkeypatch.setitem(config, "data_dir", str(tmp_path))
    (tmp_path / "mapping.txt").write_text(mapping_txt)
    (tmp_path / "mapping.py").write_text(mapping_py)

    pipeline = Pipeline.from_paths([tmp_path / "mapping.py"])
    assert len(pipeline) == 1
    pipeline.run()
    assert (tmp_path / "mapping.json").exists()
