# transformlib
![CI Checks](https://github.com/laegsgaardTroels/transformlib/workflows/CI%20Checks/badge.svg?branch=master)

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

A Transform is a many to many relationship between inputs and outputs using a function to specify the relationship transformation. 

Sample transform:

```python
# range.py
from transformlib import transform_df, Output

from pyspark.sql import SparkSession


@transform(Output('range.parquet'))
def range():
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
```

The output of one transform can be the input to another transforms.

```python
# squares.py
from transformlib import transform_df, Output, Input

from pyspark.sql import functions as F


@transform(
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
from transformlib import Pipeline
from squares import transforms

pipeline = Pipeline.discover_transforms(transforms)
```
