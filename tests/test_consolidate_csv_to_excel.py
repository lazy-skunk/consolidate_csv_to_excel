import datetime
import os
from typing import List
from unittest.mock import patch

import pandas as pd
import pytest

from src.consolidate_csv_to_excel import CSVConsolidator

_TEST_LOG_DIR: str = "test_log_dir"
_TEST_EXCEL_DIR: str = "test_excel_dir"
_TEST_EXCEL_PATH: str = os.path.join(_TEST_EXCEL_DIR, "test_output.xlsx")
_TODAY: str = (datetime.datetime.now()).strftime("%Y%m%d")
_YESTERDAY: str = (
    datetime.datetime.now() - datetime.timedelta(days=1)
).strftime("%Y%m%d")
_INVALID_DATE = "invalid-date"


@pytest.mark.parametrize(
    "input_date, expected",
    [
        (_YESTERDAY, True),
        (_INVALID_DATE, False),
    ],
)
def test_is_valid_date(input_date: str, expected: bool) -> None:
    consolidator = CSVConsolidator()
    actual = consolidator._is_valid_date(input_date)
    assert actual is expected


@pytest.mark.parametrize(
    "argv, expected, exception",
    [
        (["test.py"], _YESTERDAY, None),
        (["test.py", _TODAY], _TODAY, None),
        (["test.py", _TODAY, "HOST"], _TODAY, None),
        (
            ["test.py", _INVALID_DATE],
            None,
            SystemExit,
        ),
    ],
)
def test_get_input_date_or_yesterday(
    argv: List[str],
    expected: str,
    exception: type[SystemExit] | None,
) -> None:
    consolidator = CSVConsolidator()

    with patch("sys.argv", argv):
        if exception:
            with pytest.raises(exception):
                consolidator._get_input_date_or_yesterday()
        else:
            assert consolidator._get_input_date_or_yesterday() == expected


@pytest.mark.parametrize(
    "listdir_return_value, targets, exception",
    [
        (["host1", "host2"], ["host1", "host2"], None),
        (
            ["host1", "host2"],
            ["invalid_host"],
            SystemExit,
        ),
    ],
)
def test_validate_targets(
    listdir_return_value: List[str],
    targets: List[str],
    exception: type[SystemExit] | None,
) -> None:
    consolidator = CSVConsolidator()

    with patch("os.listdir", return_value=listdir_return_value):
        if exception:
            with pytest.raises(exception):
                consolidator._validate_targets(targets)
        else:
            consolidator._validate_targets(targets)


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["test.py", _TODAY, "host1"], ["host1"]),
        (["test.py", _TODAY, "host1,host2"], ["host1", "host2"]),
    ],
)
def test_get_targets_from_args(argv: List[str], expected: List[str]) -> None:
    consolidator = CSVConsolidator()

    with patch("sys.argv", argv):
        with patch.object(
            consolidator, "_validate_targets", return_value=None
        ):
            actual = consolidator._get_targets_from_args_or_config()
            assert actual == expected


def test_get_targets_from_config(config_path: str) -> None:
    consolidator = CSVConsolidator()

    with patch.object(consolidator, "_CONFIG_FILE_PATH", config_path):
        with patch("sys.argv", ["test.py"]):
            with patch.object(
                consolidator, "_validate_targets", return_value=None
            ):
                result = consolidator._get_targets_from_args_or_config()
                expected = ["host1", "host2"]
                assert result == expected


def test_create_output_folder_for_excel(temp_excel_folder: str) -> None:
    consolidator = CSVConsolidator()

    test_folder_path = os.path.join(temp_excel_folder, _TODAY)
    consolidator._EXCEL_FOLDER_PATH = temp_excel_folder

    assert not os.path.exists(test_folder_path)
    consolidator._create_output_folder_for_excel(_TODAY)
    assert os.path.exists(test_folder_path)

    with patch.object(consolidator._logger, "info") as mock_logger_info:
        consolidator._create_output_folder_for_excel(_TODAY)
        mock_logger_info.assert_not_called()


@pytest.mark.parametrize(
    "argv, targets, expected",
    [
        (["test.py", _TODAY, "server1"], ["server1"], "server1"),
        (
            ["test.py", _TODAY, "server1,server2"],
            ["server1", "server2"],
            "server1_server2",
        ),
        (
            ["test.py", _TODAY],
            ["server1", "server2"],
            "config",
        ),
        (
            ["test.py"],
            ["server1", "server2"],
            "config",
        ),
    ],
)
def test_determine_file_name_suffix_with_args(
    argv: List[str], targets: List[str], expected: str
) -> None:
    consolidator = CSVConsolidator()

    with patch("sys.argv", argv):
        actual = consolidator._determine_file_name_suffix(targets)
        assert actual == expected


def test_create_excel_with_sentinel_sheet(
    temp_excel_path: str,
) -> None:
    consolidator = CSVConsolidator()
    consolidator._create_excel_with_sentinel_sheet(temp_excel_path)
    assert os.path.exists(temp_excel_path)

    workbook = pd.ExcelFile(temp_excel_path)
    assert "SENTINEL_SHEET" in workbook.sheet_names


def test_abort_if_excel_exists(generated_excel_path: str) -> None:
    consolidator = CSVConsolidator()

    with pytest.raises(SystemExit):
        consolidator._create_excel_with_sentinel_sheet(generated_excel_path)


def test_get_merged_csv_path_found(temp_dir_path_with_today_csv: str) -> None:
    consolidator = CSVConsolidator()

    csv_name = f"test_{_TODAY}.csv"
    expected = os.path.join(temp_dir_path_with_today_csv, csv_name)

    actual = consolidator._get_merged_csv_path(
        temp_dir_path_with_today_csv, _TODAY
    )
    assert actual == expected


def test_get_merged_csv_path_not_found(
    temp_dir_path_with_today_csv: str,
) -> None:
    consolidator = CSVConsolidator()

    actual = consolidator._get_merged_csv_path(
        temp_dir_path_with_today_csv, _YESTERDAY
    )
    assert actual is None
