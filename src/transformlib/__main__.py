import argparse
import logging
import sys
from pathlib import Path

from transformlib import Pipeline


def main() -> None:
    """A Command Line Inferface (CLI) for running transforms."""
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
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    pipeline = Pipeline(transforms=[])
    for path in args.path:
        print(path)
        pipeline.add_transforms_from_path(path)
    pipeline.run()


if __name__ == '__main__':
    sys.exit(main())
