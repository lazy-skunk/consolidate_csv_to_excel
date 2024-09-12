import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml
from openpyxl.worksheet.worksheet import Worksheet

from src.consolidate_csvs_to_excel_by_date import (
    _EXCEL_FOLDER_PATH,
    ConfigLoader,
    CSVConsolidator,
    CSVPathMapper,
    DateHandler,
    ExcelAnalyzer,
    FileUtility,
    TargetHandler,
)

_DATE_FORMAT = "%Y%m%d"
_YESTERDAY = (datetime.now() - timedelta(days=1)).strftime(_DATE_FORMAT)
_TOMORROW = (datetime.now() + timedelta(days=1)).strftime(_DATE_FORMAT)
_TRANSPARENT = "FF"
_YELLOW = "FFFF7F"
_GRAY = "7F7F7F"
_YELLOW_WITH_TRANSPARENT = _TRANSPARENT + _YELLOW
_GRAY_WITH_TRANSPARENT = _TRANSPARENT + _GRAY


def _initialize_excel_data(file_name_without_extension: str) -> None:
    TEST_DATA_COMMON_PATH = os.path.join("tests", "data", "19880209")
    original_excel_path = os.path.join(
        TEST_DATA_COMMON_PATH, f"{file_name_without_extension}_org.xlsx"
    )
    excel_path = os.path.join(
        TEST_DATA_COMMON_PATH, f"{file_name_without_extension}.xlsx"
    )

    shutil.copy(original_excel_path, excel_path)


@pytest.mark.parametrize(
    "argv, expected",
    [
        (
            ["test.py"],
            [_YESTERDAY],
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
def test_get_date_range_or_yesterday(
    argv: List[str],
    expected: List[str],
) -> None:
    with patch.object(sys, "argv", argv):
        result = DateHandler.get_date_range_or_yesterday()
        assert result == expected


@pytest.mark.parametrize(
    "argv",
    [
        (["test.py", "1988029"]),
        (["test.py", "1988-02-09"]),
        (["test.py", "1988~02~09"]),
        (["test.py", _TOMORROW]),
        (["test.py", "invalid_date"]),
    ],
)
def test_get_date_range_or_yesterday_with_invalid_dates(
    argv: List[str],
) -> None:
    with patch.object(sys, "argv", argv):
        with pytest.raises(ValueError):
            DateHandler.get_date_range_or_yesterday()


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


@pytest.mark.parametrize(
    "argv, config_targets, expected",
    [
        (
            ["test.py", "19880209", "target1,target2"],
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
def test_get_target_prefixes(
    argv: List[str],
    config_targets: List[str] | None,
    expected: List[str],
) -> None:
    mock_config_loader = MagicMock(spec=ConfigLoader)
    if config_targets:
        mock_config_loader.get.return_value = config_targets

    with patch("sys.argv", argv):
        target_prefixes = TargetHandler.get_target_prefixes(mock_config_loader)
        assert target_prefixes == expected


@pytest.mark.parametrize(
    "target_prefixes, expected",
    [
        (["target"], ["target_0", "target_1", "target_2", "target_3"]),
        (["target_2"], ["target_2"]),
    ],
)
def test_get_target_fullnames(
    target_prefixes: List[str],
    expected: List[str],
) -> None:
    test_folders_base_path = os.path.join("tests", "data")
    with patch(
        "src.consolidate_csvs_to_excel_by_date._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        host_fullnames = TargetHandler.get_target_fullnames(target_prefixes)
        assert host_fullnames == expected


def test_get_target_fullnames_with_nonexistent_target() -> None:
    test_folders_base_path = os.path.join("tests", "data")
    with patch(
        "src.consolidate_csvs_to_excel_by_date._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        with pytest.raises(ValueError):
            TargetHandler.get_target_fullnames(["NONEXISTENT_TARGET"])


@pytest.mark.parametrize(
    "date_range, target_fullnames",
    [
        (["19880209", "19880210"], ["target_0", "target_1"]),
        (["19880209"], ["target_2"]),
    ],
)
def test_get_targets_and_csv_path_by_dates(
    date_range: List[str],
    target_fullnames: List[str],
) -> None:
    test_folders_base_path = os.path.join("tests", "data")

    expected: Dict[str, Dict[str, str]] = {}
    for date in date_range:
        expected[date] = {}
        for target_fullname in target_fullnames:
            expected[date][target_fullname] = os.path.join(
                test_folders_base_path, target_fullname, f"test_{date}.csv"
            )

    with patch(
        "src.consolidate_csvs_to_excel_by_date._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        result = CSVPathMapper.get_targets_and_csv_path_by_dates(
            date_range, target_fullnames
        )

    assert result == expected


def test_create_file_name_suffix() -> None:
    date = "19880209"
    suffix = "target_0"

    result = FileUtility.create_excel_path(date, suffix)

    expected = os.path.join(_EXCEL_FOLDER_PATH, date, f"{date}_{suffix}.xlsx")
    assert result == expected


def test_create_excel_directory(tmp_path: str) -> None:
    excel_file_path = os.path.join(tmp_path, "19880209", "19880209_file.xlsx")
    excel_directory = os.path.dirname(excel_file_path)

    assert not os.path.exists(excel_directory)
    FileUtility.create_directory(excel_file_path)
    assert os.path.exists(excel_directory)


@pytest.mark.parametrize(
    "target_folder_path, date, expected",
    [
        (
            "tests/data/target_0/",
            "19880209",
            "tests/data/target_0/test_19880209.csv",
        ),
        ("tests/data/target_4/", "19880209", None),
    ],
)
def test_get_merged_csv_path(
    target_folder_path: str,
    date: str,
    expected: str | None,
) -> None:
    result = FileUtility.get_csv_path(target_folder_path, date)
    assert result == expected


def test_consolidate_csvs_to_excel() -> None:
    date = "19880209"

    target_prefix = "target"
    target_with_csv = "target_0"
    target_with_no_csv = "target_1"
    target_with_invalid_csv = "target_2"

    csv_path = os.path.join(
        "tests", "data", target_with_csv, f"test_{date}.csv"
    )
    excel_path = os.path.join(
        "tests", "data", "output", f"{date}_{target_prefix}.xlsx"
    )

    filtered_targets_and_csv_path = {
        target_with_csv: csv_path,
        target_with_no_csv: None,
        target_with_invalid_csv: "INVALID_CSV_PATH.csv",
    }

    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as writer:
        workbook = writer.book

        csv_consolidator = CSVConsolidator(writer, workbook)
        csv_consolidator.consolidate_csvs_to_excel(
            filtered_targets_and_csv_path
        )

    added_sheets = workbook.sheetnames
    assert target_with_csv in added_sheets
    assert target_with_no_csv in added_sheets
    assert target_with_invalid_csv not in added_sheets

    no_csv_sheet = workbook[target_with_no_csv]
    assert (
        no_csv_sheet.sheet_properties.tabColor.value == _GRAY_WITH_TRANSPARENT
    )

    assert (
        target_with_invalid_csv
        in csv_consolidator.get_merge_failed_hosts()["merge_failed_hosts"]
    )


def test_highlight_cells_and_sheet_tab_by_criteria() -> None:
    def _check_cell_highlighting(
        worksheet: Worksheet, highlighted_cells: List[str]
    ) -> None:
        for row in worksheet.iter_rows():
            for cell in row:
                if cell.coordinate in highlighted_cells:
                    assert cell.fill.patternType is not None
                else:
                    assert cell.fill.patternType is None

    def _check_sheet_tab_color(
        worksheet: Worksheet, expected_color: str | None
    ) -> None:
        if expected_color:
            assert worksheet.sheet_properties.tabColor.value == expected_color
        else:
            assert worksheet.sheet_properties.tabColor is None

    date = "19880209"
    excel_path = os.path.join(
        "tests", "data", date, f"{date}_target_highlight.xlsx"
    )
    processing_time_threshold = 4

    _initialize_excel_data("19880209_target_highlight")
    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a") as writer:
        workbook = writer.book

        excel_analyzer = ExcelAnalyzer(workbook)
        excel_analyzer.highlight_cells_and_sheet_tab_by_criteria(
            processing_time_threshold
        )

        worksheet = workbook["target_0"]
        _check_cell_highlighting(worksheet, [])
        _check_sheet_tab_color(worksheet, None)

        worksheet = workbook["target_1"]
        _check_cell_highlighting(worksheet, ["D2"])
        _check_sheet_tab_color(worksheet, _YELLOW_WITH_TRANSPARENT)

        worksheet = workbook["target_2"]
        _check_cell_highlighting(worksheet, ["C2"])
        _check_sheet_tab_color(worksheet, _YELLOW_WITH_TRANSPARENT)

        worksheet = workbook["target_3"]
        _check_cell_highlighting(worksheet, ["C2", "D2"])
        _check_sheet_tab_color(worksheet, _YELLOW_WITH_TRANSPARENT)


def test_reorder_sheets_by_color() -> None:
    date = "19880209"
    excel_path = os.path.join(
        "tests", "data", date, f"{date}_target_reorder.xlsx"
    )

    _initialize_excel_data("19880209_target_reorder")
    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a") as writer:
        workbook = writer.book

        excel_analyzer = ExcelAnalyzer(workbook)
        excel_analyzer.reorder_sheets_by_color()
