"""
SparkEncoding module for PySetl.

Defines enumerations for supported Spark string encodings.
"""
from enum import Enum


class SparkEncoding(Enum):
    """
    Enumeration of available Spark string encodings.

    Attributes:
        US_ASCII: US-ASCII encoding.
        ISO_8859_1: ISO-8859-1 encoding.
        UTF_8: UTF-8 encoding.
        UTF_16BE: UTF-16 big-endian encoding.
        UTF_16LE: UTF-16 little-endian encoding.
        UTF_16: UTF-16 encoding.
    """

    US_ASCII = "US-ASCII"
    ISO_8859_1 = "ISO-8859-1"
    UTF_8 = "UTF-8"
    UTF_16BE = "UTF-16BE"
    UTF_16LE = "UTF-16LE"
    UTF_16 = "UTF-16"
