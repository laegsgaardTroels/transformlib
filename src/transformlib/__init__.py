"""Contains useful abstractions for organizing transformations of data in a modular way.
"""
from .node import Node, Output, Input
from .transform import Transform, transform
from .pipeline import Pipeline
from . import config

__all__ = [
    'Node',
    'Output', 'Input',
    'Transform', 'transform',
    'Pipeline',
    'config',
    '__version__',
]
__version__ = '0.2.5'
