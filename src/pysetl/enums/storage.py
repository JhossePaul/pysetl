from enum import Enum


class FileStorage(Enum):
    """
    Enumeration of available general file storage options.

    Attributes:
        CSV: CSV file storage.
        PARQUET: Parquet file storage.
        JSON: JSON file storage.
        GENERIC: Generic file storage.
    """

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    GENERIC = "generic"


class DbStorage(Enum):
    """
    Enumeration of available general database storage options.

    Attributes:
        JDBC: JDBC database storage.
        DELTA: Delta Lake storage.
        CASSANDRA: Cassandra database storage.
        DYNAMODB: DynamoDB storage.
        HUDI: Hudi storage.
    """

    JDBC = "JDBC"
    DELTA = "DELTA"
    CASSANDRA = "CASSANDRA"
    DYNAMODB = "DYNAMODB"
    HUDI = "HUDI"
