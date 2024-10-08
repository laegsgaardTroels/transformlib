from transformlib import configure
from transformlib._transformlib import import_and_append_to_sys_path

import pytest
import pathlib
import subprocess


EXAMPLE_DIRS = list(pathlib.Path("./examples/").glob("*"))


@pytest.mark.slow
@pytest.mark.parametrize("example_dir", EXAMPLE_DIRS)
def test_run_sh(example_dir: pathlib.Path, monkeypatch):
    """Runs all pipeline.sh shell scripts in each example/*/pipeline.sh."""
    monkeypatch.chdir(example_dir)
    subprocess.run(["/bin/bash", "pipeline.sh"], check=True)


@pytest.mark.slow
@pytest.mark.parametrize("example_dir", EXAMPLE_DIRS)
def test_run_py(example_dir: pathlib.Path, tmp_path):
    """Runs all pipeline.py python scripts in each example/*/pipeline.py."""
    pipeline = import_and_append_to_sys_path(example_dir / "pipeline.py")
    configure(data_dir=tmp_path)
    pipeline.pipeline.run()
