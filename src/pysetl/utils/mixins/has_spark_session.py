"""HasSparkSession module."""
from pyspark.sql import SparkSession
from pysetl.utils.exceptions import PySparkException


class HasSparkSession:
    """Provide current spark session property."""

    @property
    def spark(self):
        """Returns current spark session."""
        spark = SparkSession.getActiveSession()

        if spark:
            return spark
        else:
            raise PySparkException("No active Spark session")
