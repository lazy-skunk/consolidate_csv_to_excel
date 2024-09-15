import os
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from src.target_handler import ConfigLoader, TargetHandler


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
        "src.target_handler._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        host_fullnames = TargetHandler.get_target_fullnames(target_prefixes)
        assert host_fullnames == expected


def test_get_target_fullnames_with_nonexistent_target() -> None:
    test_folders_base_path = os.path.join("tests", "data")
    with patch(
        "src.target_handler._TARGET_FOLDERS_BASE_PATH",
        test_folders_base_path,
    ):
        with pytest.raises(ValueError):
            TargetHandler.get_target_fullnames(["NONEXISTENT_TARGET"])
