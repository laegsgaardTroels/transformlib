"""Utilities available with the optional transformlib[pandas] dependency."""
from transformlib._transformlib import (
    Output,
    Input,
    Parameter,
    Transform,
    Function,
    transform_read_write,
)
import typing
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None


def transform_pandas(
    *args: Output,
    **kwargs: Input | Parameter,
) -> typing.Callable[[Function], Transform]:
    """Convert a pandas function to a :py:class:`~transformlib.Transform`.

    A Transform that operates on pandas DataFrames are often constructed using the :py:func:`~transformlib.transform_pandas` decorator:

    .. highlight:: python
    .. code-block:: python

        from transformlib import transform_pandas, Output, Input
        from sklearn.tree import DecisionTreeRegressor
        import pandas as pd
        import joblib


        @transform_pandas(
            Output("model.joblib", writer=joblib.dump),
            X_train=Input(
                "X_train.csv",
                dtype={
                    "HouseAge": "float64",
                    "AveRooms": "float64",
                    "AveBedrms": "float64",
                    "Population": "float64",
                    "AveOccup": "float64",
                    "Latitude": "float64",
                    "Longitude": "float64",
                    "MedHouseVal": "float64",
                },
            ),
            y_train=Input(
                "y_train.csv",
                dtype={
                    "MedInc": "float64",
                },
            ),
        )
        def train(X_train: pd.DataFrame, y_train: pd.DataFrame) -> DecisionTreeRegressor:
            \"""Train a model and save the trained model.\"""

            # Fitting the model
            model = DecisionTreeRegressor()
            model.fit(X_train, y_train)

            return model

    In above example the ``train`` is a :py:class:`~transformlib.Transform` object that
    can be part of a :py:class:`~transformlib.Pipeline` of many pandas DataFrame transformations.

    For more see the `california housing example <https://github.com/laegsgaardTroels/transformlib/tree/master/examples/california_housing>`__.

    Args:
        *args (Output): One or more :py:class:`~transformlib.Output`\\ (s). The return value of the
            function is a single object or a tuple of objects expected to be written to args and
            with the same order as args.
        **kwargs (dict[str, Input | Parameter]): The :py:class:`~transformlib.Input`
            and :py:class:`~transformlib.Parameter` of the transform.

    Returns:
        Callable[[Function], Transform]: A decorator that returns a Transform object.

    Raises:
        ModuleNotFoundError: If pandas is not installed.
    """
    if pd is None:
        raise ModuleNotFoundError("Please install pandas")

    _default_to_pandas_csv_writer(*args)
    _default_to_pandas_csv_reader(**kwargs)
    return transform_read_write(*args, **kwargs)


def _default_to_pandas_csv_reader(**kwargs: Input | Parameter):
    if pd is None:
        raise ModuleNotFoundError("Please install pandas")
    for value in kwargs.values():
        if isinstance(value, Input) and value.reader is None:
            value.reader = _pandas_reader


def _default_to_pandas_csv_writer(*args: Output):
    for arg in args:
        if arg.writer is None:
            arg.writer = _pandas_writer


def _pandas_reader(path: Path, **metadata: typing.Any) -> typing.Any:
    return pd.read_csv(path, **metadata)


def _pandas_writer(obj: typing.Any, path: Path, **metadata: typing.Any) -> None:
    obj.to_csv(path, **metadata)
