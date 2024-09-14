import logging
import os
import sys
from logging import Logger
from logging.handlers import RotatingFileHandler

_LOG_FILE_PATH = os.path.join("log", "test.log")


class CustomLogger:  # pragma: no cover
    _instance: Logger | None = None

    @classmethod
    def get_logger(
        cls,
        log_file_path: str = _LOG_FILE_PATH,
        log_level: int = logging.INFO,
        max_file_size: int = 3 * 1024 * 1024,
        backup_count: int = 2,
    ) -> Logger:
        if cls._instance is None:
            cls._instance = cls._initialize_logger(
                log_file_path, log_level, max_file_size, backup_count
            )
        return cls._instance

    @classmethod
    def _initialize_logger(
        cls,
        log_file_path: str,
        log_level: int,
        max_file_size: int,
        backup_count: int,
    ) -> Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)

        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=max_file_size, backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger
