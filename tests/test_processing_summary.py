import logging
from typing import List
from unittest.mock import MagicMock

import pytest
from pytest import LogCaptureFixture

from src.csv_consolidator import CSVConsolidator
from src.excel_analyzer import ExcelAnalyzer
from src.processing_summary import ProcessingSummary


@pytest.mark.parametrize(
    "data_by_group_key, expected",
    [
        (
            {
                "19880209": {
                    "target_4": None,
                    "target_5": None,
                },
            },
            {"19880209": ["No CSV files found."]},
        ),
        (
            {
                "target_0": {
                    "19880214": None,
                    "19880215": None,
                },
            },
            {"target_0": ["No CSV files found."]},
        ),
    ],
)
def test_add_missing_csv_info_with_no_csv_files(
    data_by_group_key: dict[str, dict[str, str | None]],
    expected: dict[str, List[str]],
) -> None:
    processing_summary = ProcessingSummary()
    processing_summary.add_missing_csv_info(data_by_group_key)
    assert processing_summary._daily_summaries == expected


@pytest.mark.parametrize(
    "data_by_group_key, expected",
    [
        (
            {
                "19880209": {
                    "target_0": "tests/data/target_0/test_19880209.csv",
                    "target_4": None,
                    "target_5": None,
                },
            },
            {
                "19880209": [
                    "Some CSV files not found: ['target_4', 'target_5']"
                ]
            },
        ),
        (
            {
                "target_0": {
                    "19880209": "tests/data/target_0/test_19880209.csv",
                    "19880214": None,
                    "19880215": None,
                },
            },
            {
                "target_0": [
                    "Some CSV files not found: ['19880214', '19880215']"
                ]
            },
        ),
    ],
)
def test_add_missing_csv_info_with_some_csv_not_found(
    data_by_group_key: dict[str, dict[str, str | None]],
    expected: dict[str, List[str]],
) -> None:
    processing_summary = ProcessingSummary()
    processing_summary.add_missing_csv_info(data_by_group_key)
    assert processing_summary._daily_summaries == expected


@pytest.mark.parametrize(
    "data_by_group_key",
    [
        {
            "19880209": {
                "target_0": "tests/data/target_0/test_19880209.csv",
                "target_1": "tests/data/target_1/test_19880209.csv",
            },
        },
        {
            "target_0": {
                "19880209": "tests/data/target_0/test_19880209.csv",
                "19880210": "tests/data/target_0/test_19880210.csv",
            },
        },
    ],
)
def test_add_missing_csv_info_with_existing_csvs(
    data_by_group_key: dict[str, dict[str, str | None]]
) -> None:
    processing_summary = ProcessingSummary()
    processing_summary.add_missing_csv_info(data_by_group_key)
    assert processing_summary._daily_summaries == {}


def test_save_daily_processing_results() -> None:
    processing_summary = ProcessingSummary()

    csv_consolidator = MagicMock(spec=CSVConsolidator)
    excel_analyzer = MagicMock(spec=ExcelAnalyzer)

    csv_consolidator.get_merge_failed_info.return_value = {
        "merge_failed": {"target_0", "target_1"}
    }
    excel_analyzer.get_analysis_results.return_value = {
        "threshold_exceeded": {"target_2"},
        "anomaly_detected": {"target_3"},
    }

    processing_summary.save_daily_processing_results(
        "19880209", csv_consolidator, excel_analyzer
    )

    expected = {
        "19880209": {
            "merge_failed": {"target_0", "target_1"},
            "threshold_exceeded": {"target_2"},
            "anomaly_detected": {"target_3"},
        }
    }
    assert processing_summary._daily_processing_results == expected


def test_log_daily_summaries(caplog: LogCaptureFixture) -> None:
    processing_summary = ProcessingSummary()

    data_by_base_key: dict[str, dict[str, str | None]] = {
        "19880209": {
            "target_0": "tests/data/target_0/test_19880209.csv",
            "target_4": None,
            "target_5": None,
        },
    }
    processing_summary.add_missing_csv_info(data_by_base_key)

    processing_summary._daily_processing_results = {
        "19880209": {
            "threshold_exceeded": {"target_0"},
            "anomaly_detected": {"target_1"},
            "merge_failed": {"target_2", "target_3"},
        },
        "19880210": {
            "threshold_exceeded": set(),
            "anomaly_detected": set(),
            "merge_failed": set(),
        },
    }

    with caplog.at_level(logging.INFO):
        processing_summary.log_daily_summaries()

    logs = [record.getMessage() for record in caplog.records]

    assert "Starting to log summary." in logs

    assert "Summary for 19880209:" in logs
    assert "Some CSV files not found: ['target_4', 'target_5']" in logs
    assert "Exceeded threshold detected: ['target_0']" in logs
    assert "Anomaly value detected: ['target_1']" in logs
    assert "Merge failed sheets: ['target_2', 'target_3']" in logs

    assert "Summary for 19880210:" in logs
    assert "No anomalies detected." in logs

    assert "Finished logging summary." in logs
