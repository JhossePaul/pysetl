"""Writable Mixin."""
from typing_extensions import Self


class IsWritable:
    """Indicate that users can activate or deactivate write of the class."""

    _write: bool = True

    @property
    def is_writable(self) -> bool:
        """Return true if the write method will be invoked by the pipeline."""
        return self._write

    def writable(self, write: bool) -> Self:
        """
        Whether invoke the write method or not.

        @param write if set to true, then the write method of the factory will
        be invoked
        """
        self._write = write

        return self
