import argparse
import logging
import sys
from pathlib import Path

from transformlib import Pipeline

logger = logging.getLogger(__name__)


def main(paths: list[Path] | list[str], verbose: bool) -> None:
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
    paths = list(map(Path, paths))
    if verbose:
        logging.basicConfig(level=logging.INFO)
    pipeline = Pipeline.from_paths(paths=paths)
    pipeline.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run transforms found on paths.")
    parser.add_argument(
        "path",
        type=Path,
        nargs="+",
        help="One or more Paths to Transforms one wish to run in a Pipeline.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action=argparse.BooleanOptionalAction,
        help="Should the Transform be run with logging set to INFO.",
    )
    args = parser.parse_args()
    sys.exit(main(paths=args.path, verbose=args.verbose))
