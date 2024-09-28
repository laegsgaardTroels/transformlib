from transformlib import transform_pandas, Output, Input
from sklearn.tree import DecisionTreeRegressor
import pandas as pd
import joblib


@transform_pandas(
    Output("model.joblib", writer=lambda obj,
           path, **_: joblib.dump(obj, path)),
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
    """Train a model and save the trained model."""

    # Fitting the model
    model = DecisionTreeRegressor()
    model.fit(X_train, y_train)

    return model
