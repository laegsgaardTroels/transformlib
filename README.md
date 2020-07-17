# ⚒  Powertools ⚒
![CI Checks](https://github.com/laegsgaardTroels/Powertools/workflows/CI%20Checks/badge.svg?branch=master)

Enables the user to organize transformations of data with PySpark as a regular Python package.

Sample package structure:

```bash
squares
├── setup.py
└── squares
    ├── __init__.py
    ├── pipelines.py
    └── transforms
        ├── __init__.py
        ├── range.py
        └── squares.py
```

## Transform

A transformation is a node with inputs and outputs. 

Sample transform:

```python
# range.py
from powertools import transform_df, Output

from pyspark.sql import SparkSession


@transform_df(Output('range.parquet'))
def range():
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
```

One transform can be another transforms input.

```python
# squares.py
from powertools import transform_df, Output, Input

from pyspark.sql import functions as F


@transform_df(
    Output('squares.parquet'),
    range_=Input('range.parquet'),
)
def squares(range_):
    return (
        range_
        .withColumn(
            'squares',
            F.pow(F.col('id'), F.lit(2))
        )
    )
```

## Pipelines

Sample pipeline:

```python
# pipelines.py
from powertools import discover_pipeline
from squares import transforms

pipeline = discover_pipeline(transforms)
```
