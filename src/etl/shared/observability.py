import logging
import os

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "app.log")


def get_logger(name=__name__):
    """Returns a configured logger instance."""

    # Check if the log directory exists
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if already configured
    if not logger.hasHandlers():
        handler = logging.FileHandler(LOG_FILE, mode="a")
        formatter = logging.Formatter(
            "%(name)s %(asctime)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
