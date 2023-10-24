"""
SaveMode module
"""
from enum import Enum


class SaveMode(Enum):
    """
    Available Spark SaveModes
    """
    APPEND = "append"
    ERRORIFEXISTS = "errorifexists"
    IGNORE = "ignore"
    OVERWRITE = "overwrite"
