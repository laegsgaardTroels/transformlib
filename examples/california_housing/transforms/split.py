from transformlib import transform_pandas, Output, Input

import pandas as pd
from sklearn.model_selection import train_test_split


@transform_pandas(
    Output("X_train.csv"),
    Output("X_test.csv"),
    Output("y_train.csv"),
    Output("y_test.csv"),
    california_housing=Input(
        "california_housing.csv",
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
    ),
)
def split(
    california_housing: pd.DataFrame,
):
    """Splits the data into train/test."""
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

    return X_train, X_test, y_train, y_test
