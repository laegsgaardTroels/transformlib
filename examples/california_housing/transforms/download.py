from transformlib import transform, Output

from sklearn.datasets import fetch_california_housing
import pandas as pd


@transform(california_housing=Output("california_housing.csv"))
def download(california_housing):
    """Downloads the California Housing dataset."""
    housing = fetch_california_housing()
    df = pd.DataFrame(housing.data, columns=housing.feature_names)
    df[housing.target_names[0]] = housing.target
    df.to_csv(california_housing.path, header=True, index=False)
