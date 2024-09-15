import os

import pandas as pd

from src.csv_consolidator import CSVConsolidator

_TRANSPARENT = "FF"
_GRAY = "7F7F7F"
_GRAY_WITH_TRANSPARENT = _TRANSPARENT + _GRAY


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
        in csv_consolidator.get_merge_failed_info()["merge_failed"]
    )
