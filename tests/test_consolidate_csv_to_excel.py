import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Type
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.consolidate_csv_to_excel import (
    ConfigLoader,
    DateHandler,
    TargetHandler,
)

_DATE_FORMAT = "%Y%m%d"


@pytest.mark.parametrize(
    "argv, expected",
    [
        (
            ["test.py"],
            [(datetime.now() - timedelta(days=1)).strftime(_DATE_FORMAT)],
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
    mock_logger: MagicMock,
    argv: List[str],
    expected: List[str],
) -> None:
    with patch.object(sys, "argv", argv):
        date_handler = DateHandler(mock_logger)
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
                (datetime.now() + timedelta(days=1)).strftime(_DATE_FORMAT),
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


def _create_temp_config_file(tmp_path: Path, config_data: dict) -> Path:
    temp_file = tmp_path / "test_config.yaml"

    with open(temp_file, "w") as file:
        yaml.dump(config_data, file)

    return temp_file


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
    temp_config_file = _create_temp_config_file(tmp_path, config_data)
    config_loader = ConfigLoader(mock_logger, str(temp_config_file))

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
    malformed_yaml = "invalid_yaml: [unclosed list"
    temp_file = tmp_path / "malformed_config.yaml"

    with open(temp_file, "w") as file:
        file.write(malformed_yaml)

    with pytest.raises(SystemExit):
        ConfigLoader(mock_logger, str(temp_file))


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
    "host_folders, targets, expected_fullnames, exception",
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
    expected_fullnames: List[str],
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
            assert host_fullnames == expected_fullnames
