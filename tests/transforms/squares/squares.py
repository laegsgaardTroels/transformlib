from transformlib import transform, PySparkDataFrameOutput, PySparkDataFrameInput

from pyspark.sql import functions as F


@transform(
    output=PySparkDataFrameOutput('squares.parquet'),
    input_range=PySparkDataFrameInput('range.parquet'),
)
def squares(output, input_range):
    range_ = input_range.load()
    output.save(
        range_
        .withColumn(
            'squares',
            F.pow(F.col('id'), F.lit(2))
        )
    )
