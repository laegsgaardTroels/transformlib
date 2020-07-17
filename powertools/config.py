"""Configurations and constants.

This scripts should not have a dependency on ANY part of the project.
"""
import os

# The default value for ENVIRONMENT.
DEFAULT_ENVIRONMENT = 'PRODUCTION'

# The environment where the project is run. Defaults to PRODUCTION.
# Values are: TESTING, PRODUCTION
ENVIRONMENT = os.getenv('ENVIRONMENT', DEFAULT_ENVIRONMENT)

# Possible ENVIRONMENT values.
POSSIBLE_ENVIRONMENTS = ['TESTING', 'PRODUCTION', DEFAULT_ENVIRONMENT]

# The default value for BASE_PATH.
DEFAULT_BASE_PATH = '/tmp'

# Get the base paths, where data should be stored.
BASE_PATH = os.getenv('BASE_PATH', DEFAULT_BASE_PATH)

if ENVIRONMENT not in POSSIBLE_ENVIRONMENTS:
    raise ValueError(f'--- INVALID ENVIRONMENT: {ENVIRONMENT} ---')

# Change the root folder if testing.
if ENVIRONMENT == 'PRODUCTION':
    ROOT_PATH = '/'
elif ENVIRONMENT == 'TESTING':
    ROOT_PATH = '/tmp/powertools/'
else:
    raise ValueError(f'--- INVALID ENVIRONMENT: {ENVIRONMENT} ---')
