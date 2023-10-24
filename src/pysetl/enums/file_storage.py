"""FileStorage module."""
from enum import Enum


class FileStorage(Enum):
    """Available general file storage options."""

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    GENERIC = "generic"
