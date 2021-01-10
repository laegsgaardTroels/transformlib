"""Configurations.

The only config is the `ROOT_DIR` environment variable, data is saved in this directory. By default
is is `/tmp/`.
"""
import os

# The root directory where data is loaded and saved.
ROOT_DIR = os.getenv('ROOT_DIR', '/tmp/')
