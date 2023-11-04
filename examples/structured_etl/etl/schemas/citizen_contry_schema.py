from typedspark import Column
from pyspark.sql.types import StringType
from .citizen_schema import Citizen


class CitizenCountry(Citizen):
    country: Column[StringType]
