import datetime
import json
import logging
import os
import sys
from logging import Logger
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import yaml
from openpyxl import load_workbook
from openpyxl.cell import Cell
from openpyxl.styles import PatternFill


class CustomLogger:
    _LOG_FILE_PATH = os.path.join("log", "test.log")

    def __init__(
        self,
        log_file_path: str = _LOG_FILE_PATH,
        log_level: int = logging.INFO,
        max_file_size: int = 3 * 1024 * 1024,
        backup_count: int = 2,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(log_level)

        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=max_file_size, backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)

    @property
    def get_logger(self) -> Logger:
        return self._logger


class CSVConsolidator:
    _DATE_FORMAT = "%Y%m%d"
    _LOG_FOLDER_PATH = os.path.join("log_directory")
    _CONFIG_FILE_PATH = os.path.join("config", "config.yml")
    _EXCEL_FOLDER_PATH = os.path.join("output", "excel")

    def __init__(self) -> None:
        self._logger = CustomLogger().get_logger
        self._copied_count = 0
        self._no_csv_count = 0
        self._failed_count = 0
        self._failed_hosts: List[str] = []
        self._hosts_to_check: set[str] = set()
        self._daily_summaries: Dict[
            str, Dict[str, int | List[str] | Set[str]]
        ] = {}

    def _is_valid_date(self, input_date: str) -> bool:
        try:
            datetime.datetime.strptime(input_date, self._DATE_FORMAT)
            return True
        except ValueError:
            return False

    def _generate_date_range(
        self, start_date_str: str, end_date_str: str
    ) -> List[str]:
        start_date = datetime.datetime.strptime(
            start_date_str, self._DATE_FORMAT
        )
        end_date = datetime.datetime.strptime(end_date_str, self._DATE_FORMAT)

        current_date = start_date
        date_list = []
        while current_date <= end_date:
            date_list.append(current_date.strftime(self._DATE_FORMAT))
            current_date += datetime.timedelta(days=1)

        return date_list

    def _get_input_date_or_yesterday(self) -> List[str]:
        DATE = 1
        if len(sys.argv) > 1:
            input_date = sys.argv[DATE]
            if "-" in input_date:
                start_date_str, end_date_str = input_date.split("-")
                if self._is_valid_date(start_date_str) and self._is_valid_date(
                    end_date_str
                ):
                    return self._generate_date_range(
                        start_date_str, end_date_str
                    )
                else:
                    self._logger.error(
                        f"Invalid date range specified: {input_date}."
                        " Processing will be aborted."
                    )
                    sys.exit(1)
            elif self._is_valid_date(input_date):
                return [input_date]
            else:
                self._logger.error(
                    f"Invalid date specified: {input_date}."
                    " Processing will be aborted."
                )
                sys.exit(1)
        else:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return [yesterday.strftime(self._DATE_FORMAT)]

    def _validate_targets(self, targets: List[str]) -> None:
        for target in targets:
            if not any(
                folder.startswith(target)
                for folder in os.listdir(self._LOG_FOLDER_PATH)
            ):
                self._logger.error(
                    f"No folder starting with target '{target}' was found"
                    f" in the log directory '{self._LOG_FOLDER_PATH}'."
                    " Processing will be aborted."
                )
                sys.exit(1)

    def _get_targets_from_args_or_config(self) -> List[str]:
        TARGET = 2
        if len(sys.argv) > 2:
            targets = sys.argv[TARGET].split(",")
        else:
            with open(self._CONFIG_FILE_PATH, "r") as file:
                config = yaml.safe_load(file)

            targets = config.get("targets", [])

        self._validate_targets(targets)
        return targets

    def _get_processing_time_threshold(self) -> int:
        with open(self._CONFIG_FILE_PATH, "r") as file:
            config = yaml.safe_load(file)

        threshold = config.get("processing_time_threshold_seconds")

        if isinstance(threshold, int):
            return threshold
        else:
            self._logger.error(
                "Invalid value for 'processing_time_threshold_seconds'"
                " in config file. Please provide a valid integer value."
                " Processing will be aborted."
            )
            sys.exit(1)

    def _create_output_folder_for_excel(self, date: str) -> None:
        date_folder = os.path.join(self._EXCEL_FOLDER_PATH, date)

        if not os.path.exists(date_folder):
            os.makedirs(date_folder)
            self._logger.info(f"Created directory: {date_folder}")

    def _determine_file_name_suffix(self, targets: List[str]) -> str:
        if len(sys.argv) > 2:
            return "_".join(targets)
        else:
            return "config"

    def _create_excel_with_sentinel_sheet(self, excel_path: str) -> None:
        # if os.path.exists(excel_path):
        #     self._logger.warning(
        #         f"Excel file '{excel_path}' already exists."
        #         " Processing will be aborted."
        #     )
        #     sys.exit(1)

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame({"A": ["SENTINEL_SHEET"]}).to_excel(
                writer, sheet_name="SENTINEL_SHEET", index=False, header=False
            )

        self._logger.info(
            f"Initialized {excel_path} with a sentinel sheet"
            " for further writing."
        )

    def _get_merged_csv_path(
        self, host_folder_path: str, date: str
    ) -> Optional[str]:
        csv_name = f"test_{date}.csv"
        csv_path = os.path.join(host_folder_path, csv_name)

        if os.path.exists(csv_path):
            self._logger.info(f"{csv_path} is found.")
            return csv_path
        else:
            self._logger.info(f"{csv_path} is not found.")
            return None

    def _copy_csv_to_excel(
        self, writer: pd.ExcelWriter, csv_path: str, host_name: str
    ) -> None:
        try:
            df = pd.read_csv(csv_path)
            df.to_excel(writer, sheet_name=host_name, index=False)
            self._logger.info(
                f"Added '{host_name}' sheet from file: {csv_path}."
            )
            self._copied_count += 1
        except Exception as e:
            self._logger.error(f"Failed to read CSV file at {csv_path}: {e}")
            self._logger.info(f"Skipping {host_name} sheet due to error.")
            self._failed_count += 1
            self._failed_hosts.append(host_name)

    def _create_no_csv_sheet_to_excel(
        self, writer: pd.ExcelWriter, host_name: str
    ) -> None:
        df_for_not_found = pd.DataFrame({"A": ["No CSV file found."]})
        df_for_not_found.to_excel(
            writer, sheet_name=host_name, index=False, header=False
        )

        _GRAY = "808080"
        writer.sheets[host_name].sheet_properties.tabColor = _GRAY

        self._logger.info(
            f"Wrote 'No CSV file found.' in cell A1 of '{host_name}' sheet."
        )
        self._no_csv_count += 1

    def _add_sheet_for_target(
        self, writer: pd.ExcelWriter, host_folder_path: str, date: str
    ) -> None:
        csv_file_path = self._get_merged_csv_path(host_folder_path, date)
        host_name = os.path.basename(host_folder_path)

        if csv_file_path:
            self._copy_csv_to_excel(writer, csv_file_path, host_name)
        else:
            self._create_no_csv_sheet_to_excel(writer, host_name)

    def _search_and_append_csv_to_excel(
        self,
        date: str,
        targets: List[str],
        excel_path: str,
    ) -> None:
        with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a") as writer:
            for host_folder_name in os.listdir(self._LOG_FOLDER_PATH):
                if not any(
                    host_folder_name.startswith(target) for target in targets
                ):
                    continue

                host_folder_path = os.path.join(
                    self._LOG_FOLDER_PATH, host_folder_name
                )
                if os.path.isdir(host_folder_path):
                    self._add_sheet_for_target(writer, host_folder_path, date)

    def _remove_sentinel_sheet(self, excel_path: str) -> None:
        workbook = load_workbook(excel_path)
        if "SENTINEL_SHEET" in workbook.sheetnames:
            del workbook["SENTINEL_SHEET"]
            workbook.save(excel_path)
            self._logger.info(f"Removed SENTINEL_SHEET from {excel_path}.")
            return

        self._logger.warning(f"SENTINEL_SHEET not found in {excel_path}.")

    def _highlight_cell(self, cell: Cell, color_code: str) -> None:
        pattern_fill = PatternFill(start_color=color_code, fill_type="solid")
        cell.fill = pattern_fill

    def _calculate_color_based_on_excess_ratio(
        self, processing_time_seconds: int, threshold: int
    ) -> str:
        excess_ratio = (processing_time_seconds - threshold) / threshold
        clamped_excess_ratio = min(excess_ratio, 1)

        _MAX_GREEN_VALUE = 255
        _MIN_GREEN_VALUE = _MAX_GREEN_VALUE / 2
        green_value = int(
            _MAX_GREEN_VALUE
            - (_MAX_GREEN_VALUE - _MIN_GREEN_VALUE) * clamped_excess_ratio
        )

        green_hex_value = f"{green_value:02X}"
        color_code = f"FF{green_hex_value}7F"

        return color_code

    def _check_and_highlight_processing_time(
        self,
        row: Tuple[Cell, ...],
        processing_time_column: int,
        threshold: int,
    ) -> bool:
        processing_time_cell = row[processing_time_column]
        processing_time_value = processing_time_cell.value

        if processing_time_value:
            try:
                processing_time_seconds = int(
                    processing_time_value.rstrip("s")
                )

                if processing_time_seconds >= threshold:
                    color_code = self._calculate_color_based_on_excess_ratio(
                        processing_time_seconds, threshold
                    )
                    self._highlight_cell(processing_time_cell, color_code)
                    return True
            except ValueError:
                self._logger.warning(
                    f"Invalid processing time value: {processing_time_value}"
                )

        return False

    def _check_and_highlight_json_key(
        self,
        row: tuple[Cell, ...],
        json_column: int,
    ) -> bool:
        json_cell = row[json_column]
        json_value = json_cell.value

        _LIGHT_YELLOW = "FFFF7F"
        if json_value:
            try:
                json_data = json.loads(json_value)
                if any(item.get("random_key") is True for item in json_data):
                    self._highlight_cell(json_cell, _LIGHT_YELLOW)
                    return True
            except json.JSONDecodeError:
                self._logger.warning(
                    f"Invalid JSON format found: {json_value}"
                )
        return False

    def _highlight_cells_and_sheet_tabs_by_criteria(
        self, excel_path: str, threshold: int
    ) -> None:
        self._logger.info("Analyze and highlight started.")
        workbook = load_workbook(excel_path)

        _HEADER_ROW = 1
        _DATA_START_ROW = _HEADER_ROW + 1
        _ZERO_BASED_INDEX_OFFSET = 1
        _PROCESSING_TIME_COLUMN = 3 - _ZERO_BASED_INDEX_OFFSET
        _JSON_COLUMN = 4 - _ZERO_BASED_INDEX_OFFSET
        _LIGHT_YELLOW = "FFFF7F"
        total_sheets = len(workbook.sheetnames)
        for current_sheet_number, host_name in enumerate(
            workbook.sheetnames, start=1
        ):
            self._logger.info(f"Processing sheet: {host_name}")
            sheet = workbook[host_name]
            has_highlighted_cell = False

            for row in sheet.iter_rows(min_row=_DATA_START_ROW):
                if self._check_and_highlight_processing_time(
                    row, _PROCESSING_TIME_COLUMN, threshold
                ) or self._check_and_highlight_json_key(row, _JSON_COLUMN):
                    self._hosts_to_check.add(host_name)
                    has_highlighted_cell = True

            if has_highlighted_cell:
                sheet.sheet_properties.tabColor = _LIGHT_YELLOW

            self._logger.info(
                f"Completed processing sheet: {host_name}."
                f" ({current_sheet_number}/{total_sheets})"
            )

        workbook.save(excel_path)
        workbook.close()
        self._logger.info("Analyze and highlight completed for all sheets.")

    def _save_daily_summary(self, date: str) -> None:
        self._daily_summaries[date] = {
            "copied": self._copied_count,
            "no_csv": self._no_csv_count,
            "failed": self._failed_count,
            "failed_hosts": self._failed_hosts,
            "hosts_to_check": self._hosts_to_check,
        }

    def _reset_counters_and_collections(self) -> None:
        self._copied_count = 0
        self._no_csv_count = 0
        self._failed_count = 0
        self._failed_hosts = []
        self._hosts_to_check = set()

    def _log_daily_summary(self) -> None:
        self._logger.info("Daily summary of processing:")
        for date, summary in self._daily_summaries.items():
            self._logger.info(
                f"Date: {date} - Copied: {summary['copied']},"
                f" No CSV: {summary['no_csv']}. "
            )

            if summary.get("hosts_to_check"):
                self._logger.warning(
                    f"Date: {date} - hosts to check:"
                    f" {', '.join(summary['hosts_to_check'])}"
                )

            if summary.get("failed"):
                self._logger.error(
                    f"Date: {date} - Failed: {summary['failed']},"
                    f" Failed Hosts: {', '.join(summary['failed_hosts'])}"
                )

    def main(self) -> None:
        self._logger.info("Process started.")

        date_list = self._get_input_date_or_yesterday()
        targets = self._get_targets_from_args_or_config()
        processing_time_threshold = self._get_processing_time_threshold()

        for date in date_list:
            file_name_suffix = self._determine_file_name_suffix(targets)
            excel_name = f"{date}_{file_name_suffix}.xlsx"
            excel_path = os.path.join(
                self._EXCEL_FOLDER_PATH, date, excel_name
            )

            self._create_output_folder_for_excel(date)
            self._create_excel_with_sentinel_sheet(excel_path)
            self._search_and_append_csv_to_excel(date, targets, excel_path)
            self._remove_sentinel_sheet(excel_path)

            self._highlight_cells_and_sheet_tabs_by_criteria(
                excel_path, processing_time_threshold
            )

            self._save_daily_summary(date)
            self._reset_counters_and_collections()

        self._log_daily_summary()
        self._logger.info("Process completed.")


if __name__ == "__main__":  # pragma: no cover
    consolidator = CSVConsolidator()
    consolidator.main()
