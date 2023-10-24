"""
DbStorage module
"""
from enum import Enum


class DbStorage(Enum):
    """
    Available general DB storage options
    """
    DELTA = "DELTA"
    CASSANDRA = "CASSANDRA"
    DYNAMODB = "DYNAMODB"
    HUDI = "HUDI"
