from transformlib import config

import pytest
import subprocess

from examples.california_housing.pipeline import pipeline as california_housing_pipeline


@pytest.mark.parametrize(
    "example_dir",
    [
        "california_housing",
    ],
)
def test_run_sh(example_dir: str):
    subprocess.call(["/bin/bash", f"./examples/{example_dir}/pipeline.sh"])


@pytest.mark.parametrize(
    "pipeline",
    [
        california_housing_pipeline,
    ],
)
def test_run_py(tmp_path, pipeline):
    config["data_dir"] = tmp_path
    pipeline.run()
