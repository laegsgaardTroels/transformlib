from transformlib import transform, Output, Input
import pandas as pd
from sklearn import metrics

import joblib


@transform(
    eval_output=Output("eval.csv"),
    model_input=Input("model.joblib"),
    X_test_input=Input("X_test.csv"),
    y_test_input=Input("y_test.csv"),
    X_train_input=Input("X_train.csv"),
    y_train_input=Input("y_train.csv"),
)
def eval(
    eval_output, model_input, X_test_input, y_test_input, X_train_input, y_train_input
):
    """Evaluate a saved model on the testing data."""

    # Load a trained model.
    model = joblib.load(model_input.path)

    # Loading the training data.
    X_train = pd.read_csv(
        X_train_input.path,
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
    )
    y_train = pd.read_csv(
        y_train_input.path,
        dtype={
            "MedInc": "float64",
        },
    )

    # Loading the testing data.
    X_test = pd.read_csv(
        X_test_input.path,
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
    )
    y_test = pd.read_csv(
        y_test_input.path,
        dtype={
            "MedInc": "float64",
        },
    )

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Save evaluation.
    pd.DataFrame(
        [
            {
                "mae_train": metrics.mean_absolute_error(y_train, y_pred_train),
                "mae_test": metrics.mean_absolute_error(y_test, y_pred_test),
            }
        ]
    ).to_csv(eval_output.path, header=True, index=False)
