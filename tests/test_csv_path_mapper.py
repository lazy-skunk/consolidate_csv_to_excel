import os
from typing import Dict, List
from unittest.mock import patch

import pytest

from src.csv_path_mapper import CSVPathMapper


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
        "src.csv_path_mapper._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        result = CSVPathMapper.get_targets_and_csv_paths_by_dates(
            date_range, target_fullnames
        )

    assert result == expected


@pytest.mark.parametrize(
    "date_range, target_fullnames",
    [
        (["19880209", "19880210"], ["target_0", "target_1"]),
        (["19880209"], ["target_2"]),
    ],
)
def test_get_csv_path_for_each_date_by_targets(
    date_range: List[str],
    target_fullnames: List[str],
) -> None:
    test_folders_base_path = os.path.join("tests", "data")

    expected: Dict[str, Dict[str, str]] = {}
    for target_fullname in target_fullnames:
        expected[target_fullname] = {}
        for date in date_range:
            expected[target_fullname][date] = os.path.join(
                test_folders_base_path, target_fullname, f"test_{date}.csv"
            )

    with patch(
        "src.csv_path_mapper._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        result = CSVPathMapper.get_csv_path_for_each_date_by_targets(
            date_range, target_fullnames
        )

    assert result == expected
