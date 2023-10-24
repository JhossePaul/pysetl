"""
utils.mixins module.

Collection of class mixins to add functionalities to base
package classes
"""
from .can_create import CanCreate
from .can_delete import CanDelete
from .can_drop import CanDrop
from .can_partition import CanPartition
from .can_update import CanUpdate
from .can_vaccum import CanVaccum
from .can_wait import CanWait
from .has_benchmark import HasBenchmark
from .has_diagram import HasDiagram
from .has_logger import HasLogger
from .has_reader import HasReader
from .has_reader_writer import HasReaderWriter
from .has_registry import HasRegistry
from .has_spark_session import HasSparkSession
from .has_writer import HasWriter
from .is_identifiable import IsIdentifiable
from .is_writable import IsWritable
from .is_configurable import IsConfigurable


__all__ = [
    "CanCreate",
    "CanDelete",
    "CanDrop",
    "CanPartition",
    "CanUpdate",
    "CanVaccum",
    "CanWait",
    "HasBenchmark",
    "HasDiagram",
    "HasLogger",
    "HasReader",
    "HasReaderWriter",
    "HasRegistry",
    "HasSparkSession",
    "HasWriter",
    "IsConfigurable",
    "IsIdentifiable",
    "IsWritable"
]
