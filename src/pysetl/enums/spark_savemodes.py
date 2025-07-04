"""
SaveMode module for PySetl.

Defines enumerations for supported Spark DataFrame save modes.
"""
from enum import Enum


class SaveMode(Enum):
    """
    Enumeration of available Spark DataFrame save modes.

    Attributes:
        APPEND: Append data to an existing dataset.
        ERRORIFEXISTS: Throw an error if the dataset exists.
        IGNORE: Ignore the operation if the dataset exists.
        OVERWRITE: Overwrite the existing dataset.
    """

    APPEND = "append"
    ERRORIFEXISTS = "errorifexists"
    IGNORE = "ignore"
    OVERWRITE = "overwrite"
