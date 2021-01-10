from transformlib import transform, Output, Input

from pyspark.sql import SparkSession


@transform(
    Output('range.parquet'),
    squares=Input('squares.parquet'),
)
def range(squares):
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
