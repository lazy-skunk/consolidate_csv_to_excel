import datetime
from typing import List
from unittest.mock import patch

import pytest

from src.consolidate_csv_to_excel import CSVConsolidator

_TEST_LOG_DIR: str = "test_log_dir"
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
    "argv, expected, exception, exception_message",
    [
        (["test.py"], _YESTERDAY, None, None),
        (["test.py", _TODAY], _TODAY, None, None),
        (["test.py", _TODAY, "HOST"], _TODAY, None, None),
        (
            ["test.py", _INVALID_DATE],
            None,
            ValueError,
            f"Invalid date specified: {_INVALID_DATE}.",
        ),
    ],
)
def test_get_input_date_or_yesterday(
    argv: List[str],
    expected: str,
    exception: type[ValueError] | None,
    exception_message: str,
) -> None:
    consolidator = CSVConsolidator()

    with patch("sys.argv", argv):
        if exception:
            with pytest.raises(exception, match=exception_message):
                consolidator._get_input_date_or_yesterday()
        else:
            assert consolidator._get_input_date_or_yesterday() == expected


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["test.py", _TODAY, "host1"], ["host1"]),
        (["test.py", _TODAY, "host1,host2"], ["host1", "host2"]),
    ],
)
def test_get_targets_from_args(argv: List[str], expected: str) -> None:
    consolidator = CSVConsolidator()

    with patch("sys.argv", argv):
        actual = consolidator._get_targets_from_args_or_config()
        assert actual == expected


def test_get_targets_from_config(temp_config_file_path: str) -> None:
    consolidator = CSVConsolidator()

    with patch.object(
        consolidator, "_CONFIG_FILE_PATH", temp_config_file_path
    ):
        with patch("sys.argv", ["test.py"]):
            result = consolidator._get_targets_from_args_or_config()
            expected = ["host1", "host2"]
            assert result == expected


@pytest.mark.parametrize(
    "listdir_return_value, targets, exception, exception_message",
    [
        (["host1", "host2"], ["host1", "host2"], None, None),
        (
            ["host1", "host2"],
            ["invalid_host"],
            ValueError,
            "No folder matching target 'invalid_host'",
        ),
    ],
)
def test_validate_targets(
    listdir_return_value: List[str],
    targets: List[str],
    exception: type[ValueError] | None,
    exception_message: str | None,
) -> None:
    consolidator = CSVConsolidator()

    with patch("os.listdir", return_value=listdir_return_value):
        if exception:
            with pytest.raises(exception, match=exception_message):
                consolidator._validate_targets(_TEST_LOG_DIR, targets)
        else:
            consolidator._validate_targets(_TEST_LOG_DIR, targets)


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
