import logging
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_logger() -> MagicMock:
    logger = MagicMock(spec=logging.Logger)
    return logger
