import logging


class Logging:
    def logInfo(self, msg: str) -> None:
        logging.info(msg)

    def logDebug(self, msg: str) -> None:
        logging.debug(msg)

    def logWarn(self, msg: str) -> None:
        logging.warning(msg)

    def logError(self, msg: str) -> None:
        logging.error(msg)
