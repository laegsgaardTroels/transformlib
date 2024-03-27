from transformlib import transform, Output, Input

import pandas as pd
from sklearn.model_selection import train_test_split


@transform(
    X_train_output=Output("X_train.csv"),
    y_train_output=Output("y_train.csv"),
    X_test_output=Output("X_test.csv"),
    y_test_output=Output("y_test.csv"),
    california_housing_input=Input("california_housing.csv"),
)
def split(
    X_train_output: Output,
    y_train_output: Output,
    X_test_output: Output,
    y_test_output: Output,
    california_housing_input: Input,
):
    """Splits the data into train/test."""

    california_housing = pd.read_csv(
        california_housing_input.path,
        dtype={
            "MedInc": "float64",
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
    X = california_housing[
        [
            "MedInc",
            "HouseAge",
            "AveRooms",
            "AveBedrms",
            "Population",
            "AveOccup",
            "Latitude",
            "Longitude",
        ]
    ]
    y = california_housing["MedHouseVal"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.33, random_state=42
    )

    X_train.to_csv(X_train_output.path, header=True, index=False)
    X_test.to_csv(X_test_output.path, header=True, index=False)
    y_train.to_csv(y_train_output.path, header=True, index=False)
    y_test.to_csv(y_test_output.path, header=True, index=False)
