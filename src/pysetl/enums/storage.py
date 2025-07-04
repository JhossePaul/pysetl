from enum import Enum


class FileStorage(Enum):
    """Available general file storage options."""

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    GENERIC = "generic"


class DbStorage(Enum):
    """
    Available general DB storage options
    """

    JDBC = "JDBC"
    DELTA = "DELTA"
    CASSANDRA = "CASSANDRA"
    DYNAMODB = "DYNAMODB"
    HUDI = "HUDI"
