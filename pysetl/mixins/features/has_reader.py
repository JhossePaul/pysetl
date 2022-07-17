"""
HasReaderMixin module
"""
from pyspark.sql import DataFrameReader


class HasReaderMixin:
    _reader: DataFrameReader
