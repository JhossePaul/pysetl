"""Dummy Schemas for testing purposes."""
from typedspark import Schema, Column
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, TimestampType, DateType
)

class DummyData(Schema):
    """Dummy schema for testing purposes."""

    language: Column[StringType]
    users_count: Column[StringType]


class DSSchema(Schema):
    """Class for testing purposes."""

    partition1: Column[IntegerType]
    partition2: Column[StringType]
    clustering1: Column[StringType]
    value: Column[IntegerType]


class DS3Schema(Schema):
    """Class for testing purposes."""

    partition1: Column[IntegerType]
    partition2: Column[StringType]
    clustering1: Column[StringType]
    value: Column[IntegerType]
    value2: Column[StringType]


class DS2Schema(Schema):
    """Class for testing purposes."""

    col1: Column[StringType]
    col2: Column[IntegerType]
    col3: Column[FloatType]
    col4: Column[TimestampType]
    col5: Column[DateType]
    col6: Column[IntegerType]


DUMMY_DATA = {
    "DSSchema": [
        {
            DSSchema.partition1: 1, DSSchema.partition2: "a",
            DSSchema.clustering1: "A", DSSchema.value: 1
        },
        {
            DSSchema.partition1: 2, DSSchema.partition2: "b",
            DSSchema.clustering1: "B", DSSchema.value: 2
        }
    ]
}
