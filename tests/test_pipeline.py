import pytest

from transformlib import (
    Transform,
    Pipeline,
    Input,
    Output
)
from transformlib.pipeline import (
    TransformlibDuplicateTransformException,
    TransformlibCycleException,
    TransformlibDuplicateInputException,
    TransformlibDuplicateOutputException,
    _tsort
)


def test_run_duplicate_transforms():
    """Should raise exception if duplicate Transform."""
    transform = Transform(
        output_kwargs={},
        func=lambda: None,
        input_kwargs={},
    )
    with pytest.raises(TransformlibDuplicateTransformException):
        pipeline = Pipeline([transform, transform])
        pipeline.run()


def test_run_transform_exception_handling():
    """Should raise an exception if one is raised in a Transform."""
    class TransformlibTestRunTasksException(Exception):
        """Raised in this test case."""

    def raise_transform_exception():
        raise TransformlibTestRunTasksException('Transform test.')

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
        output_kwargs={'foo_input': Output('foo')},
        func=func1,
        input_kwargs={'bar_input': Input('bar')},
    )
    transform2 = Transform(
        output_kwargs={'bar_output': Output('bar')},
        func=func2,
        input_kwargs={'foo_input': Input('foo')},
    )
    pipeline = Pipeline([transform1, transform2])
    with pytest.raises(TransformlibCycleException):
        pipeline.run()


def test_run_duplicate_output_exception():
    transform1 = Transform(
        output_kwargs={'foo_output': Output('foo')},
        func=lambda foo_output: None,
        input_kwargs={},
    )
    transform2 = Transform(
        output_kwargs={'foo_output': Output('foo')},
        func=lambda foo_output: None,
        input_kwargs={},
    )
    pipeline = Pipeline([transform1, transform2])
    with pytest.raises(TransformlibDuplicateOutputException):
        pipeline.run()


def test_run_duplicate_input_exception():
    transform1 = Transform(
        output_kwargs={},
        func=lambda foo_output: None,
        input_kwargs={'foo_input': Output('foo')},
    )
    transform2 = Transform(
        output_kwargs={},
        func=lambda foo_output: None,
        input_kwargs={'foo_input': Input('foo')},
    )
    pipeline = Pipeline([transform1, transform2])
    with pytest.raises(TransformlibDuplicateInputException):
        pipeline.run()


def test_discover_transforms(tmp_path, monkeypatch):
    """"Test that the pipeline runs."""
    monkeypatch.setenv('TRANSFORMLIB_DATA_DIR', str(tmp_path))
    (tmp_path / 'mapping.txt').write_text("""1,2
3,4
5,6
7,8
9,10""")
    from tests import transforms
    pipeline = Pipeline.discover_transforms(transforms)
    pipeline.run()
    assert (tmp_path / 'mapping.json').exists()


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
