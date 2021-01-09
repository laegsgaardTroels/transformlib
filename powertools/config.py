"""Configurations.

This scripts should not have a dependency on ANY part of the project.
"""
import os

# The root directory where data is loaded and saved.
ROOT_DIR = os.getenv('ROOT_DIR', '/tmp/')
