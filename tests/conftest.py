import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.consolidate_csv_to_excel import (
    ConfigLoader,
    CSVConsolidator,
    DateHandler,
    ExcelAnalyzer,
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
def excel_analyzer(mock_logger: MagicMock) -> ExcelAnalyzer:
    return ExcelAnalyzer(mock_logger)


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
    random_keys = [None, True, None, True]
    for i in range(4):
        data = []
        valid_data = []
        invalid_data = []

        csv_file = f"{tmp_path}/target_{i}/test_19880209.csv"

        csv_directory = os.path.dirname(csv_file)
        os.makedirs(csv_directory, exist_ok=True)

        date_a = datetime.now() - timedelta(seconds=i)
        date_b = datetime.now() + timedelta(seconds=i)
        processing_time = int((date_b - date_a).total_seconds())
        json_list = [
            {
                "date_a": date_a.isoformat(),
                "date_b": date_b.isoformat(),
                "time_difference": f"{processing_time}s",
                "random_key": random_keys[i],
            }
        ]

        stringified_json = json.dumps(json_list)
        valid_data.append(
            [
                date_a.strftime("%Y-%m-%d %H:%M:%S"),
                date_b.strftime("%Y-%m-%d %H:%M:%S"),
                f"{processing_time}s",
                stringified_json,
            ]
        )

        invalid_data.append(
            [
                date_a.strftime("%Y-%m-%d %H:%M:%S"),
                date_b.strftime("%Y-%m-%d %H:%M:%S"),
                "INVALID_PROCESSING_TIME",
                "INVALID_JSON",
            ]
        )

        data = valid_data + invalid_data

        df = pd.DataFrame(
            data, columns=["date_a", "date_b", "processing_time", "random_key"]
        )
        df.to_csv(csv_file, index=False)


@pytest.fixture
def prepare_tmp_excel(
    prepare_tmp_csv: None,
    tmp_path: Path,
    tmp_path_for_excel: str,
) -> None:
    with pd.ExcelWriter(
        tmp_path_for_excel, engine="openpyxl", mode="w"
    ) as writer:
        for i in range(4):
            csv_file = f"{tmp_path}/target_{i}/test_19880209.csv"
            df = pd.read_csv(csv_file)
            sheet_name = f"target_{i}"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
