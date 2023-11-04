from typedspark import Schema, Column
from pyspark.sql.types import StringType


class City(Schema):
    city: Column[StringType]
    country: Column[StringType]
