from transformlib import transform, PySparkDataFrameOutput

from pyspark.sql import SparkSession


@transform(output=PySparkDataFrameOutput('range.parquet'))
def range(output):
    spark = SparkSession.builder.getOrCreate()
    output.save(spark.range(100))
