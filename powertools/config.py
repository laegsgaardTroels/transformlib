"""Configurations and constants.

This scripts should not have a dependency on ANY part of the project.
"""
import os

# The environment where the project is run. Defaults to PRODUCTION.
# Values are: TESTING, PRODUCTION. The default value for ENVIRONMENT.
ENVIRONMENT = os.getenv('ENVIRONMENT', 'PRODUCTION')

# Possible ENVIRONMENT values.
POSSIBLE_ENVIRONMENTS = ['TESTING', 'PRODUCTION']

# Get the ROOT_PATH to where data should be stored.
# The default value for ROOT_PATH is /tmp/.
ROOT_PATH = os.getenv('ROOT_PATH', '/tmp/')

if ENVIRONMENT not in POSSIBLE_ENVIRONMENTS:
    raise ValueError(f'--- INVALID ENVIRONMENT: {ENVIRONMENT} ---')
