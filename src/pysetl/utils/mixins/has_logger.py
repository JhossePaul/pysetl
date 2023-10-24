"""HasLogger module."""
import logging


class HasLogger:
    """Mixin to provide logging capabilities to a class."""

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
