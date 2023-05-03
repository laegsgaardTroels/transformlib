import argparse
from typing import List
import logging
import sys
from pathlib import Path

from transformlib import Pipeline

logger = logging.getLogger(__name__)


def main(path: List[Path], verbose: bool) -> None:
    """A Command Line Inferface (CLI) for running :py:class:`transformlib.Transform`.

    Example usage:

    .. highlight:: bash
    .. code-block:: bash

        transform path/to/transforms/*.py
        transform -v path/to/transforms/*.py

    The transform command is install from setup.py when installing the package.

    Args:
        path: (List[Path]): One or more Paths to Transforms one wish to run in a Pipeline.
        verbose (bool): Should the Transform be run with logging set to INFO.
    """
    if verbose:
        logging.basicConfig(level=logging.INFO)
    pipeline = Pipeline(transforms=[])
    for path in map(Path, path):
        if path.exists():
            logger.info(f"Adding transforms from {path}")
            pipeline.add_transforms_from_path(path)
        else:
            raise FileNotFoundError(f"No file found at {path}")
    pipeline.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run transforms found on paths.'
    )
    parser.add_argument(
        'path',
        type=Path,
        nargs='+',
        help="One or more Paths to Transforms one wish to run in a Pipeline.",
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action=argparse.BooleanOptionalAction,
        help="Should the Transform be run with logging set to INFO.",
    )
    args = parser.parse_args()
    sys.exit(main(path=args.path, verbose=args.verbose))
