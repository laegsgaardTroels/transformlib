from powertools import transform_df, Output, Input

from pyspark.sql import functions as F


@transform_df(
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
