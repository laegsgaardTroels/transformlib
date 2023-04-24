from transformlib import config

from typing import Union
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class Node:
    """The `Node` base class is a node in a directed asyclic graph (DAG) of data transformations."""

    def __init__(
        self,
        path: Union[str, Path],
        root_dir: Union[str, Path] = config.ROOT_DIR,
    ):
        self.path = path
        self.root_dir = root_dir

    @property
    def _path(self):
        return Path(self.root_dir) / self.path

    def __repr__(self):
        return f'{self.__class__.__name__}(path={self.path})'

    def __str__(self):
        return self.path

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(self.path)


class Output(Node):
    """An `Output` is a sink in a DAG of data transformations."""

    def __init__(
        self,
        path: str,
        root_dir: str = config.ROOT_DIR,
        **write_kwargs
    ):
        super().__init__(path, root_dir)
        self.write_kwargs = write_kwargs

    def save(self, obj, **save_kwargs):
        """Saves an object containing data to the `config.ROOT_DIR`."""
        raise NotImplementedError(
            f"The save method is not implemented for {self.__class__.__name__}"
        )


class Input(Node):
    """An `Input` is a source in a DAG of data transformations."""

    def __init__(
        self,
        path: str,
        root_dir: str = config.ROOT_DIR,
        **load_kwargs
    ):
        super().__init__(path, root_dir)
        self.load_kwargs = load_kwargs

    def load(self, obj, **load_kwargs) -> None:
        """Loads an object containing data from the `config.ROOT_DIR`."""
        raise NotImplementedError(
            f"The load method is not implemented for {self.__class__.__name__}"
        )


class PySparkDataFrameOutput(Output):
    """Used for PySpark DataFrame Output of a Transform."""

    def save(self, df: 'pyspark.sql.DataFrame', **write_kwargs) -> None:
        """Save and output PySpark DataFrame from a fixed path.

        The default save mode is set to 'overwrite' because this is most
        commonly used.

        >>> from transformlib import PySparkDataFrameOutput
        >>> PySparkDataFrameOutput('/path/to/output.csv', format='csv')
        PySparkDataFrameOutput(path=/path/to/output.csv)

        Args:
            df (pyspark.sql.DataFrame): A DataFrame which is to be saved in the output
                location.
            **write_kwargs: The key value arguments to the DataFrameReader class
                in pyspark.sql.
        """
        write_kwargs = write_kwargs or self.write_kwargs
        if 'mode' not in write_kwargs:
            write_kwargs['mode'] = 'overwrite'
        df.write.save(path=self.path, **write_kwargs)


class PySparkDataFrameInput(Input):
    """Used for PySpark DataFrame Input to a Transform."""

    def load(self, **load_kwargs) -> 'pyspark.sql.DataFrame':
        """Load an input PySpark DataFrame from a fixed path.

        Used to create input to be read.

        >>> from transformlib import Input
        >>> PySparkDataFrameInput('/path/to/input.csv', format='csv')
        PySparkDataFrameInput(path=/path/to/input.csv)

        Args:
            **read_kwargs: Kwargs to the DataFrameReader class from pyspark.sql.

        Returns:
            pyspark.sql.DataFrame: The DataFrame that the input is pointing to.
        """
        import pyspark
        spark = pyspark.sql.SparkSession.builder.getOrCreate()
        load_kwargs = load_kwargs or self.load_kwargs
        return spark.read.load(path=self.path, **load_kwargs)


class PandasDataFrameOutput(Output):
    """Used for pandas DataFrame Output of a Transform."""

    def save(self, df: 'pandas.DataFrame', format='csv', **write_kwargs) -> None:
        """Save the output Pandas DataFrame from a fixed path.

        >>> from transformlib import PandasDataFrameOutput
        >>> PandasDataFrameOutput('/path/to/input.csv', format='csv')
        PandasDataFrameOutput(path=/path/to/input.csv)

        Args:
            df (pandas.DataFrame): A pandas DataFrame which is to be saved in the
                output location.
            **write_kwargs: The key value arguments to the DataFrameReader class
                in pyspark.sql.
        """
        if format == 'csv':
            df.to_csv(self._path, **write_kwargs)
        elif format == 'parquet':
            df.to_parquet(self._path, **write_kwargs)
        else:
            raise NotImplementedError(f"The save method is not implemented for {format}")


class PandasDataFrameInput(Input):
    """Used for pandas DataFrame Input to a Transform."""

    def load(self, format='csv', **load_kwargs) -> 'pandas.DataFrame':
        """Load an input Pandas DataFrame from a fixed path.

        >>> from transformlib import PandasDataFrameInput
        >>> PandasDataFrameInput('/path/to/output.csv')
        PandasDataFrameInput(path=/path/to/output.csv)

        Returns:
            pandas.DataFrame: The DataFrame that the input is pointing to.
        """
        import pandas as pd
        if format == 'csv':
            return pd.read_csv(self._path, **load_kwargs)
        elif format == 'parquet':
            return pd.read_parquet(self._path, **load_kwargs)
        else:
            raise NotImplementedError(f"The save method is not implemented for {format}")