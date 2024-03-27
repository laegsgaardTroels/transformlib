from transformlib import transform, Output, Input
from sklearn.tree import DecisionTreeRegressor
import pandas as pd
import joblib


@transform(
    model_output=Output("model.joblib"),
    X_train_input=Input("X_train.csv"),
    y_train_input=Input("y_train.csv"),
)
def train(model_output, X_train_input, y_train_input):
    """Train a model and save the trained model."""

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

    # Fitting the model
    model = DecisionTreeRegressor()
    model.fit(X_train, y_train)

    # Save a trained model.
    joblib.dump(model, model_output.path)
