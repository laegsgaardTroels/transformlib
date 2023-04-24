from transformlib import transform, PySparkDataFrameOutput, PySparkDataFrameInput

from pyspark.sql import SparkSession


@transform(
    output=PySparkDataFrameOutput('range.parquet'),
    squares=PySparkDataFrameInput('squares.parquet'),
)
def range(output, input_squares):
    spark = SparkSession.builder.getOrCreate()
    output.save(spark.range(100))
