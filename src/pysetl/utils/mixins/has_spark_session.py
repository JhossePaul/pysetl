"""HasSparkSession module."""
from pyspark.sql import SparkSession


class HasSparkSession:
    """Provide current spark session property."""

    @property
    def spark(self):
        """Returns current spark session."""
        return SparkSession.Builder().getOrCreate()
