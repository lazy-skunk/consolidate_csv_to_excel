import logging
import os
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.consolidate_csv_to_excel import (
    ConfigLoader,
    CSVConsolidator,
    DateHandler,
    TargetHandler,
)


@pytest.fixture
def mock_logger() -> MagicMock:
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def date_handler(mock_logger: MagicMock) -> DateHandler:
    return DateHandler(mock_logger)


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


@pytest.fixture
def tmp_path_for_excel(tmp_path: Path) -> str:
    return os.path.join(tmp_path, "output.xlsx")


@pytest.fixture
def prepare_tmp_excel_with_sentinel(tmp_path_for_excel: str) -> None:
    with pd.ExcelWriter(
        tmp_path_for_excel, engine="openpyxl", mode="w"
    ) as writer:
        pd.DataFrame({"A": ["SENTINEL_SHEET"]}).to_excel(
            writer, sheet_name="SENTINEL_SHEET", index=False, header=False
        )


@pytest.fixture
def prepare_tmp_excel_with_sentinel_and_dummy(
    tmp_path_for_excel: str, prepare_tmp_excel_with_sentinel: None
) -> None:
    with pd.ExcelWriter(
        tmp_path_for_excel, engine="openpyxl", mode="a"
    ) as writer:
        pd.DataFrame({"A": ["VISIBLE_SHEET"]}).to_excel(
            writer, sheet_name="VISIBLE_SHEET", index=False, header=False
        )


@pytest.fixture
def prepare_tmp_csv(tmp_path: Path) -> None:
    for i in range(3):
        csv_file = f"{tmp_path}/target_{i}/test_19880209.csv"

        csv_directory = os.path.dirname(csv_file)
        os.makedirs(csv_directory, exist_ok=True)

        df = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})
        df.to_csv(csv_file, index=False)
