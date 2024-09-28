from transformlib import transform_pandas, Output, Input
import pandas as pd
from sklearn import metrics

import joblib


@transform_pandas(
    Output("eval.csv"),
    model=Input("model.joblib", reader=lambda path, **_: joblib.load(path)),
    X_test=Input(
        "X_test.csv",
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
    y_test=Input(
        "y_test.csv",
        dtype={
            "MedInc": "float64",
        },
    ),
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
def eval(model, X_test, y_test, X_train, y_train):
    """Evaluate a saved model on the testing data."""

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    return pd.DataFrame(
        [
            {
                "mae_train": metrics.mean_absolute_error(y_train, y_pred_train),
                "mae_test": metrics.mean_absolute_error(y_test, y_pred_test),
            }
        ]
    )
