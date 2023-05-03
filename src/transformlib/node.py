import os
from pathlib import Path
from typing import Optional
from typing import Union

from transformlib import config


class Node:
    """The Node base class is a node in a directed asyclic graph (DAG) of data transformations."""

    def __init__(
        self,
        relative_path: Union[str, Path],
        data_dir: Optional[Union[str, Path]] = None,
    ):
        self.relative_path = relative_path
        if data_dir is None:
            self.data_dir = os.getenv(config.DATA_DIR, '/tmp/')
        else:
            self.data_dir = data_dir

    @property
    def path(self) -> Path:
        """The path to the node in the directory :py:const:`config.DATA_DIR`."""
        return Path(self.data_dir) / self.relative_path

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.relative_path})'

    def __eq__(self, other) -> bool:
        return self.path == other.path

    def __hash__(self) -> bool:
        return hash(self.path)


class Output(Node):
    """An Output is a sink in a DAG of data transformations."""
    pass


class Input(Node):
    """An Input is a source in a DAG of data transformations."""
    pass
