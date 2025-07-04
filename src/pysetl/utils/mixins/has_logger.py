"""
HasLogger mixin for PySetl.

Provides an abstract mixin to add logging functionality to classes.
"""
import logging


class HasLogger:
    """
    Abstract mixin to add logging functionality to a class.

    This class cannot be instantiated directly.
    """

    def log_info(self, msg: str) -> None:
        """Log info level message."""
        logging.info(msg)

    def log_debug(self, msg: str) -> None:
        """Log debug level message."""
        logging.debug(msg)

    def log_warning(self, msg: str) -> None:
        """Log warning level message."""
        logging.warning(msg)

    def log_error(self, msg: str) -> None:
        """Log error level message."""
        logging.error(msg)

    def log(self, message: str) -> None:
        """
        Abstract method to log a message from the class or workflow.

        Args:
            message (str): The message to log.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
