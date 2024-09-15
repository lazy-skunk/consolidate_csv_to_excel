import os
import shutil
from typing import List

import pandas as pd
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from src.excel_analyzer import ExcelAnalyzer

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


def test_highlight_cells_and_sheet_tabs_by_criteria() -> None:
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
        excel_analyzer.highlight_cells_and_sheet_tabs_by_criteria(
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

        worksheet = workbook["no_csv"]
        _check_cell_highlighting(worksheet, [])
        _check_sheet_tab_color(worksheet, _GRAY_WITH_TRANSPARENT)


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


def test_get_analysis_results() -> None:
    workbook = Workbook()
    excel_analyzer = ExcelAnalyzer(workbook)

    excel_analyzer._threshold_exceeded_sheets = {"target_1", "target_3"}
    excel_analyzer._anomaly_detected_sheets = {"target_2"}

    expected = {
        "threshold_exceeded": {"target_1", "target_3"},
        "anomaly_detected": {"target_2"},
    }

    actual = excel_analyzer.get_analysis_results()
    assert actual == expected
