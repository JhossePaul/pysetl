from typedspark import Schema, Column
from pyspark.sql.types import StringType, IntegerType


class Citizen(Schema):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]
