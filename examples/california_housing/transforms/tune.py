from transformlib import transform_pandas, Output, Input
from sklearn.model_selection import GridSearchCV
from sklearn.tree import DecisionTreeRegressor
import pandas as pd


@transform_pandas(
    Output("scores.csv"),
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
def tune(X_train: pd.DataFrame, y_train: pd.DataFrame) -> pd.DataFrame:
    """Evaluate the model using different hyperparameters."""  # Loading the training data.

    # Conducting a grid search cross validation over the specified grid of parameters.
    model = DecisionTreeRegressor()
    gs = GridSearchCV(
        model,
        {
            "max_depth": [7, 9],
            "min_samples_split": [10, 20],
        },
        scoring="neg_mean_absolute_error",
        n_jobs=-1,
        cv=5,
    )
    gs.fit(X_train, y_train)

    return pd.DataFrame(
        [
            {"mean_test_score": mean_test_score, "params": params}
            for mean_test_score, params in zip(
                gs.cv_results_["mean_test_score"],
                gs.cv_results_["params"],
            )
        ]
    )
