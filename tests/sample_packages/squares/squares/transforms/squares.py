from transformlib import transform, Output, Input

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
