from transformlib import transform_pandas, Output

from sklearn.datasets import fetch_california_housing
import pandas as pd


@transform_pandas(Output("california_housing.csv"))
def download():
    """Downloads the California Housing dataset."""
    housing = fetch_california_housing()
    california_housing = pd.DataFrame(
        housing.data, columns=housing.feature_names)
    california_housing[housing.target_names[0]] = housing.target
    return california_housing
