import os
from typing import Any, Dict

import yaml

from src.custom_logger import CustomLogger

_CONFIG_FILE_PATH = os.path.join("config", "config.yml")


class ConfigLoader:
    _logger = CustomLogger.get_logger()

    def __init__(self, config_file_path: str = _CONFIG_FILE_PATH):
        self._config_file_path = config_file_path
        self._config: Dict[str, Any] = {}

    def _load_config(self) -> None:
        try:
            with open(self._config_file_path, "r") as file:
                self._config = yaml.safe_load(file)
            self._logger.info(
                f"Configuration file {self._config_file_path}"
                " loaded successfully."
            )
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Configuration file {self._config_file_path} not found."
            )
        except yaml.YAMLError as e:
            raise yaml.YAMLError(
                f"Error parsing {self._config_file_path}: {e}."
            )

    def get(self, key: str, default: Any = None) -> Any:
        if not self._config:
            self._load_config()
        return self._config.get(key, default)

    def get_processing_time_threshold(self) -> int:
        threshold = self.get("processing_time_threshold_seconds")

        if isinstance(threshold, int):
            return threshold
        else:
            raise ValueError(
                "Invalid value for 'processing_time_threshold_seconds'"
                " in config file."
            )
