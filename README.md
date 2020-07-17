![CI Checks](https://github.com/laegsgaardTroels/Powertools/workflows/CI%20Checks/badge.svg?branch=master)

# ⚒  Powertools ⚒

Enables the user to organize transformations of data with PySpark as a regular Python package.

Sample package structure:

```bash
.
├── tests
├── setup.py
└── my_package
    ├── __init__.py
    ├── pipelines.py
    └── transforms
        ├── __init__.py
        ├── range.py
        └── square.py
```

## Transform

A transformation is a node with inputs and outputs. 

Sample transform:

```python
# range.py
from powertools import transform_df, Output, Input

from pyspark.sql import SparkSession

@transform_df(Output('range.parquet'))
def range():
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
```

One transform can be another transforms input.

```python
# square.py
from powertools import transform_df, Output, Input

@transform_df(
    Output('squares.parquet'),
    range=Input('range.parquet'),
)
def square(range):
    return range.withColumn('squares', F.pow(F.col('id'), F.lit(2)))
```

## Pipelines

Sample pipeline:

```python
# pipelines.py
from powertools import discover_pipeline
import transforms

pipeline = discover_pipeline(transforms)
```
