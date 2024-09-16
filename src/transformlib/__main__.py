import argparse
import logging
from pathlib import Path

from transformlib import configure, Pipeline


def main() -> None:
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
    parser = argparse.ArgumentParser(
        description="Run transforms found on paths.")
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
    parser.add_argument(
        "-d",
        "--data-dir",
        default="/tmp/",
        help="Directory where data is saved.",
    )
    args = parser.parse_args()

    configure(data_dir=args.data_dir)
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    pipeline = Pipeline.from_paths(paths=list(map(Path, args.path)))
    pipeline.run()
