from powertools import transform_df, Output

from pyspark.sql import SparkSession


@transform_df(Output('range.parquet'))
def range():
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
