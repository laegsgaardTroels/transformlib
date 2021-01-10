from powertools import transform, Output

from pyspark.sql import SparkSession


@transform(Output('range.parquet'))
def range():
    spark = SparkSession.builder.getOrCreate()
    return spark.range(100)
