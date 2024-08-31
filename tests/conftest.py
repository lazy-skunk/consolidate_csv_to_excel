import logging
from unittest.mock import MagicMock

import pytest

from src.consolidate_csv_to_excel import (
    ConfigLoader,
    CSVConsolidator,
    TargetHandler,
)


@pytest.fixture
def mock_logger() -> MagicMock:
    logger = MagicMock(spec=logging.Logger)
    return logger


@pytest.fixture
def mock_config_loader() -> MagicMock:
    return MagicMock(spec=ConfigLoader)


@pytest.fixture
def target_handler(
    mock_config_loader: MagicMock, mock_logger: MagicMock
) -> TargetHandler:
    return TargetHandler(mock_config_loader, mock_logger)


@pytest.fixture
def csv_consolidator(mock_logger: MagicMock) -> CSVConsolidator:
    return CSVConsolidator(mock_logger)
