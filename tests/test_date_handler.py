import sys
from datetime import datetime, timedelta
from typing import List
from unittest.mock import patch

import pytest

from src.date_handler import DateHandler

_DATE_FORMAT = "%Y%m%d"
_YESTERDAY = (datetime.now() - timedelta(days=1)).strftime(_DATE_FORMAT)
_TOMORROW = (datetime.now() + timedelta(days=1)).strftime(_DATE_FORMAT)


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
