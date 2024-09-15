from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def patch_target_folders_base_path(
    tmp_path: Path,
) -> Generator[None, None, None]:
    with patch(
        "src.csv_path_mapper._TARGET_FOLDERS_BASE_PATH",
        tmp_path,
    ):
        yield


# @pytest.fixture
# def prepare_tmp_excel_with_sentinel_and_dummy(
#     tmp_path_for_excel: str, prepare_tmp_excel_with_sentinel: None
# ) -> Generator[pd.ExcelWriter, None, None]:
#     with pd.ExcelWriter(
#         tmp_path_for_excel, engine="openpyxl", mode="a"
#     ) as writer:
#         pd.DataFrame({"A": ["VISIBLE_SHEET"]}).to_excel(
#             writer, sheet_name="VISIBLE_SHEET", index=False, header=False
#         )
#         yield writer


# @pytest.fixture
# def prepare_tmp_excel(
#     prepare_tmp_csv: None,
#     tmp_path: Path,
#     tmp_path_for_excel: str,
# ) -> None:
#     with pd.ExcelWriter(
#         tmp_path_for_excel, engine="openpyxl", mode="w"
#     ) as writer:
#         for i in range(4):
#             csv_file = f"{tmp_path}/target_{i}/test_19880209.csv"
#             df = pd.read_csv(csv_file)
#             sheet_name = f"target_{i}"
#             df.to_excel(writer, sheet_name=sheet_name, index=False)


# @pytest.fixture
# def prepare_tmp_excel_for_reordering(tmp_path_for_excel: Path) -> None:
#     try:
#         workbook = Workbook()

#         default_sheet = workbook.active
#         workbook.remove(default_sheet)

#         workbook.create_sheet("Other_Sheet")

#         gray_sheet = workbook.create_sheet("Gray_Sheet")
#         gray_sheet.sheet_properties.tabColor = "007F7F7F"

#         yellow_sheet = workbook.create_sheet("Yellow_Sheet")
#         yellow_sheet.sheet_properties.tabColor = "00FFFF7F"

#         workbook.save(tmp_path_for_excel)
#     finally:
#         workbook.close()
