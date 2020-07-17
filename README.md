![CI Checks](https://github.com/laegsgaardTroels/Powertools/workflows/CI%20Checks/badge.svg?branch=master)

# ⚒  Powertools ⚒

Used to organize transformations of data with PySpark. The transformations can be organized as a package with `powertools` as a dependency.

Sample package structure:

```bash
.
├── tests
├── setup.py
└── my_pyspark_package
    ├── pipelines.py
    └── transforms
        ├── __init__.py
        ├── transform_one.py
        ├── transform_two.py
        ├── . 
        ├── . 
        ├── . 
        └── transform_n.py
```

## Transform

A transformation is a node with inputs and outputs. 

Sample transform:

```python
# transform_one.py
from powertools import transform_df, Output, Input

from pyspark.sql import SparkSession

@transform_df(Output('range.parquet'))
def transform_one():
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
```

One transform can be another transforms input.

```python
# transform_two.py
from powertools import transform_df, Output, Input

@transform_df(
    Output('squares.parquet'),
    range=Input('range.parquet'),
)
def transform_two(range):
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
