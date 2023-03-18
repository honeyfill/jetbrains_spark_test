from unittest import TestCase
from unittest import main
import warnings

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType

from pyspark.sql import SparkSession

from src.etl import produce_session_id

from datetime import datetime as dt


INPUT_SHEMA = StructType(
    [
        StructField('user_id', StringType(), False),
        StructField('event_id', StringType(), False),
        StructField('timestamp', TimestampType(), False),
        StructField('product_code', StringType(), False)
    ]
)

OUTPUT_SHEMA = StructType(
    [
        StructField('user_id', StringType(), False),
        StructField('timestamp', TimestampType(), False),
        StructField('product_code', StringType(), False),
        StructField('session_id', StringType(), False),
    ]
)

TESTING_TIMESTAMP = dt.fromtimestamp(1625309785.482347)
ANOTHER_TIMESTAMP = TESTING_TIMESTAMP.replace(year=2024)

class ETLProcessTest(TestCase):

    def __init__(self, methodName: str = "runTest") -> None:
        self.spark_session = SparkSession.builder.getOrCreate()
        super().__init__(methodName)

    def test_duplicate(self) -> None:

        input_data = [
            ('u1', 'a', TESTING_TIMESTAMP, 'PyCharm'),
            ('u1', 'b', TESTING_TIMESTAMP, 'PyCharm')
        ]
        input_df = self.spark_session.createDataFrame(
            data=input_data,
            schema=INPUT_SHEMA
        )
        processed_df = produce_session_id(input_df)

        expected_data = [
            ('u1', TESTING_TIMESTAMP, 'PyCharm', 'u1#PyCharm#' + str(TESTING_TIMESTAMP))
        ]
        expected_df = self.spark_session.createDataFrame(
            data=expected_data,
            schema=OUTPUT_SHEMA
        )

        self.assertEqual(expected_df.collect(), processed_df.collect())


    def test_not_user_event(self) -> None:

        input_data = [
            ('u1', 'e', TESTING_TIMESTAMP, 'PyCharm'),
            ('u1', '1', TESTING_TIMESTAMP, 'PyCharm')
        ]
        input_df = self.spark_session.createDataFrame(
            data=input_data,
            schema=INPUT_SHEMA
        )
        processed_df = produce_session_id(input_df)

        expected_data = []
        expected_df = self.spark_session.createDataFrame(
            data=expected_data,
            schema=OUTPUT_SHEMA
        )

        self.assertEqual(expected_df.collect(), processed_df.collect())


    def test_two_users(self) -> None:

        input_data = [
            ('u1', 'a', TESTING_TIMESTAMP, 'PyCharm'),
            ('u2', 'b', TESTING_TIMESTAMP, 'IDEA')
        ]
        input_df = self.spark_session.createDataFrame(
            data=input_data,
            schema=INPUT_SHEMA
        )
        processed_df = produce_session_id(input_df)

        expected_data = [
            ('u1', TESTING_TIMESTAMP, 'PyCharm', 'u1#PyCharm#' + str(TESTING_TIMESTAMP)),
            ('u2', TESTING_TIMESTAMP, 'IDEA', 'u2#IDEA#' + str(TESTING_TIMESTAMP))
        ]
        expected_df = self.spark_session.createDataFrame(
            data=expected_data,
            schema=OUTPUT_SHEMA
        )

        self.assertEqual(expected_df.collect(), processed_df.collect())


    def test_different_timestamps(self) -> None:

        input_data = [
            ('u1', 'a', TESTING_TIMESTAMP, 'PyCharm'),
            ('u1', 'b', ANOTHER_TIMESTAMP, 'PyCharm')
        ]
        input_df = self.spark_session.createDataFrame(
            data=input_data,
            schema=INPUT_SHEMA
        )
        processed_df = produce_session_id(input_df)

        expected_data = [
            ('u1', TESTING_TIMESTAMP, 'PyCharm', 'u1#PyCharm#' + str(TESTING_TIMESTAMP)),
            ('u1', ANOTHER_TIMESTAMP, 'PyCharm', 'u1#PyCharm#' + str(ANOTHER_TIMESTAMP))
        ]
        expected_df = self.spark_session.createDataFrame(
            data=expected_data,
            schema=OUTPUT_SHEMA
        )

        self.assertEqual(sorted(expected_df.collect()), sorted(processed_df.collect()))


    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)


if __name__ == '__main__':
    main()
