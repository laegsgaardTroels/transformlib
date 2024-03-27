from transformlib import transform, Output, Input
from sklearn.model_selection import GridSearchCV
from sklearn.tree import DecisionTreeRegressor
import pandas as pd


@transform(
    scores_output=Output("scores.csv"),
    X_train_input=Input("X_train.csv"),
    y_train_input=Input("y_train.csv"),
)
def tune(scores_output, X_train_input, y_train_input):
    """Evaluate the model using different hyperparameters."""

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
    
    # Saving the scores.
    pd.DataFrame(
        [
            {"mean_test_score": mean_test_score, "params": params}
            for mean_test_score, params in zip(
                gs.cv_results_["mean_test_score"],
                gs.cv_results_["params"],
            )
        ]
    ).to_csv(scores_output.path, header=True, index=False)
