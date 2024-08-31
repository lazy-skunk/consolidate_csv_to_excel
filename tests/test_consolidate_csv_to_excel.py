import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Type
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from src.consolidate_csv_to_excel import (
    ConfigLoader,
    CSVConsolidator,
    DateHandler,
    TargetHandler,
)


class TestHelper:
    DATE_FORMAT = "%Y%m%d"
    YESTERDAY = (datetime.now() - timedelta(days=1)).strftime(DATE_FORMAT)
    TODAY = datetime.now().strftime(DATE_FORMAT)
    TOMORROW = (datetime.now() + timedelta(days=1)).strftime(DATE_FORMAT)

    @staticmethod
    def create_temp_config_file_and_return_path(
        tmp_path: Path, config_data: dict
    ) -> str:
        temp_config_path = os.path.join(tmp_path, "temp_config.yaml")

        with open(temp_config_path, "w") as file:
            yaml.dump(config_data, file)

        return temp_config_path

    @staticmethod
    def create_malformed_config_and_return_path(tmp_path: Path) -> str:
        malformed_content = "invalid_yaml: [unclosed list"
        temp_file = os.path.join(tmp_path, "malformed_config.yaml")

        with open(temp_file, "w") as file:
            file.write(malformed_content)

        return temp_file


@pytest.mark.parametrize(
    "argv, expected",
    [
        (
            ["test.py"],
            [TestHelper.YESTERDAY],
        ),
        (["test.py", "19880209"], ["19880209"]),
        (["test.py", "19880209~19880209"], ["19880209"]),
        (
            ["test.py", "19880209~19880211"],
            ["19880209", "19880210", "19880211"],
        ),
        (
            ["test.py", "19880211~19880209"],
            ["19880209", "19880210", "19880211"],
        ),
    ],
)
def test_get_input_date_or_yesterday(
    date_handler: MagicMock,
    argv: List[str],
    expected: List[str],
) -> None:
    with patch.object(sys, "argv", argv):
        result = date_handler.get_input_date_or_yesterday()
        assert result == expected


@pytest.mark.parametrize(
    "argv",
    [
        (["test.py", "1988029"]),
        (["test.py", "1988-02-09"]),
        (["test.py", "1988~02~09"]),
        (
            [
                "test.py",
                TestHelper.TOMORROW,
            ]
        ),
        (["test.py", "invalid_date"]),
    ],
)
def test_get_input_date_or_yesterday_invalid_dates(
    mock_logger: MagicMock, argv: List[str]
) -> None:
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            date_handler = DateHandler(mock_logger)
            date_handler.get_input_date_or_yesterday()


@pytest.mark.parametrize(
    "config_data, expected, exception",
    [
        ({"processing_time_threshold_seconds": 30}, 30, None),
        (
            {"processing_time_threshold_seconds": "invalid"},
            None,
            SystemExit,
        ),
        ({}, None, SystemExit),
    ],
)
def test_get_processing_time_threshold(
    mock_logger: MagicMock,
    tmp_path: Path,
    config_data: dict,
    expected: int,
    exception: type[SystemExit] | None,
) -> None:
    temp_config_path = TestHelper.create_temp_config_file_and_return_path(
        tmp_path, config_data
    )
    config_loader = ConfigLoader(mock_logger, str(temp_config_path))

    if exception:
        with pytest.raises(exception):
            config_loader.get_processing_time_threshold()
    else:
        threshold = config_loader.get_processing_time_threshold()
        assert threshold == expected


def test_config_not_found(mock_logger: MagicMock) -> None:
    with pytest.raises(SystemExit):
        ConfigLoader(mock_logger, "non_existent_file.yaml")


def test_get_processing_time_threshold_with_invalid_config(
    mock_logger: MagicMock, tmp_path: Path
) -> None:
    malformed_yml_path = TestHelper.create_malformed_config_and_return_path(
        tmp_path
    )
    with pytest.raises(SystemExit):
        ConfigLoader(mock_logger, str(malformed_yml_path))


@pytest.mark.parametrize(
    "argv, config_targets, expected",
    [
        (
            ["test.py", "dummy_arg", "target1,target2"],
            None,
            ["target1", "target2"],
        ),
        (
            ["test.py"],
            ["config_target1", "config_target2"],
            ["config_target1", "config_target2"],
        ),
    ],
)
def test_get_targets(
    target_handler: TargetHandler,
    mock_config_loader: MagicMock,
    argv: List[str],
    config_targets: List[str] | None,
    expected: List[str],
) -> None:
    with patch("sys.argv", argv):
        if config_targets is not None:
            mock_config_loader.get.return_value = config_targets

        targets = target_handler.get_targets()
        assert targets == expected


@pytest.mark.parametrize(
    "host_folders, targets, expected, exception",
    [
        (
            ["host1_log", "host2_log", "host3_log"],
            ["host1", "host2"],
            ["host1_log", "host2_log"],
            None,
        ),
        (
            ["host1_log", "host2_log", "host3_log"],
            ["host4"],
            [],
            SystemExit,
        ),
        (
            ["host1_log", "host2_log", "host3_log"],
            ["host1", "host4"],
            ["host1_log"],
            None,
        ),
    ],
)
def test_get_existing_host_fullnames(
    target_handler: TargetHandler,
    host_folders: List[str],
    targets: List[str],
    expected: List[str],
    exception: Type[SystemExit] | None,
) -> None:
    with patch("os.listdir", return_value=host_folders):
        if exception:
            with pytest.raises(exception):
                target_handler.get_existing_host_fullnames(targets)
        else:
            host_fullnames = target_handler.get_existing_host_fullnames(
                targets
            )
            assert host_fullnames == expected


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["test.py", "arg1", "arg2"], "target1_target2"),
        (["test.py"], "config"),
    ],
)
def test_create_excel_file_path(
    csv_consolidator: CSVConsolidator,
    tmp_path: Path,
    argv: List[str],
    expected: str,
) -> None:
    date = "19880209"
    targets = ["target1", "target2"]

    with (
        patch("sys.argv", argv),
        patch(
            "src.consolidate_csv_to_excel._EXCEL_FOLDER_PATH",
            tmp_path,
        ),
    ):
        expected = f"{tmp_path}/{date}/{date}_{expected}.xlsx"
        result = csv_consolidator.create_excel_file_path(date, targets)
        assert result == expected


def test_create_excel_with_sentinel_sheet(
    csv_consolidator: CSVConsolidator, tmp_excel_path: str
) -> None:
    csv_consolidator.create_excel_with_sentinel_sheet(tmp_excel_path)
    assert Path(tmp_excel_path).exists()

    with pytest.raises(SystemExit):
        csv_consolidator.create_excel_with_sentinel_sheet(tmp_excel_path)


def test_search_and_append_csv_to_excel(
    csv_consolidator: CSVConsolidator,
    prepare_tmp_csv: None,
    prepare_excel_with_sentinel: None,
    tmp_path: Path,
    tmp_excel_path: str,
) -> None:
    date = "19880209"
    target_fullnames = [f"target_{i}" for i in range(3)]

    with patch(
        "src.consolidate_csv_to_excel._TARGET_FOLDERS_BASE_PATH", f"{tmp_path}"
    ):
        csv_consolidator.search_and_append_csv_to_excel(
            date, target_fullnames, tmp_excel_path
        )

        with pd.ExcelFile(tmp_excel_path) as xls:
            assert set(xls.sheet_names) == {
                "SENTINEL_SHEET",
                "target_0",
                "target_1",
                "target_2",
            }
