from pyspark.sql.functions import concat_ws
from pyspark.sql import DataFrame


def produce_session_id(dataframe) -> DataFrame:

    result = dataframe \
        .filter(
            (dataframe.event_id == 'a') |
            (dataframe.event_id == 'b') |
            (dataframe.event_id == 'c')
        ) \
        .drop(
            dataframe.event_id
        ) \
        .dropDuplicates() \
        .withColumn(
            'session_id',
            concat_ws('#', dataframe.user_id, dataframe.product_code, dataframe.timestamp)
        )

    return result
