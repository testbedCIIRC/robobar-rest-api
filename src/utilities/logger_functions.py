"""Utility functions for logging."""

import logging
import sys
from pathlib import Path


def get_logger(
    *,
    name: str = __name__,
    log_path: str | Path = "./",
    file_name: str = "debug",
    to_file: bool = True,
    level: int = logging.DEBUG,
) -> logging.Logger:
    """Create and return a logger instance."""
    if not isinstance(log_path, Path):
        log_path = Path(log_path)
    logger = logging.getLogger(name)
    logger.handlers.clear()

    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  [%(filename)s:%(funcName)s] %(message)s",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(level)
    logger.addHandler(stream_handler)
    if to_file:
        log_path.mkdir(parents=True, exist_ok=True)
        full_path = log_path.absolute() / f"{file_name}.log"
        file_handler = logging.FileHandler(full_path)
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
        info_msg = f"Logger created at {full_path}."
        logger.info(info_msg)
    else:
        logger.info("Logger created.")

    logger.setLevel(level)

    return logger


logger: logging.Logger = get_logger(
    name="auth_api_logger",
    log_path="./logs",
    file_name="robobar_api_log",
    to_file=False,
    level=logging.DEBUG,
)
