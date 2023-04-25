import pytest

from transformlib import config


@pytest.fixture(autouse=True)
def configure(monkeypatch, tmp_path):
    """Set the data directory for all tests in this file."""
    monkeypatch.setenv(config.DATA_DIR, str(tmp_path))
