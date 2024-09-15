import logging
import os
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.config_loader import ConfigLoader


def test_get_processing_time_threshold() -> None:
    temp_config_path = os.path.join("tests", "data", "test_config.yml")
    config_loader = ConfigLoader(temp_config_path)
    mock_logger = MagicMock(spec=logging.Logger)

    with patch.object(config_loader, "_logger", mock_logger):
        threshold = config_loader.get_processing_time_threshold()
        expected = 4
        assert threshold == expected


def test_get_processing_time_threshold_with_nonexistent_file() -> None:
    mock_logger = MagicMock(spec=logging.Logger)
    config_loader = ConfigLoader("NONEXISTENT_CONFIG.YAML")

    with patch.object(config_loader, "_logger", mock_logger):
        with pytest.raises(FileNotFoundError):
            config_loader.get_processing_time_threshold()


def test_get_processing_time_threshold_with_invalid_config() -> None:
    invalid_config_path = os.path.join("tests", "data", "invalid_yaml.yml")
    config_loader = ConfigLoader(invalid_config_path)
    mock_logger = MagicMock(spec=logging.Logger)

    with patch.object(config_loader, "_logger", mock_logger):
        with pytest.raises(yaml.YAMLError):
            config_loader.get_processing_time_threshold()


def test_get_processing_time_threshold_with_invalid_threshold() -> None:
    invalid_threshold_path = os.path.join(
        "tests", "data", "invalid_threshold.yml"
    )
    config_loader = ConfigLoader(invalid_threshold_path)
    mock_logger = MagicMock(spec=logging.Logger)

    with patch.object(config_loader, "_logger", mock_logger):
        with pytest.raises(ValueError):
            config_loader.get_processing_time_threshold()
