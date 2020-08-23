from powertools import transform_df, Output, Input

from pyspark.sql import SparkSession


@transform_df(
    Output('range.parquet'),
    squares=Input('squares.parquet'),
)
def range(squares):
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
