"""HasReaderWriter module."""
from abc import ABCMeta
from .has_reader import HasReader
from .has_writer import HasWriter


class HasReaderWriter(HasReader, HasWriter, metaclass=ABCMeta):
    """Mixin to provide SparkReader and SparkWriter properties."""
