"""
HasSparkSession mixin for PySetl.

Provides an abstract mixin to add SparkSession functionality to classes.
"""
from pyspark.sql import SparkSession


class HasSparkSession:
    """
    Abstract mixin to add SparkSession functionality to a class.

    This class cannot be instantiated directly.
    """

    @property
    def spark(self) -> SparkSession:
        """
        Get the SparkSession instance.

        Returns:
            SparkSession: The SparkSession instance.
        """
        return SparkSession.Builder().getOrCreate()
