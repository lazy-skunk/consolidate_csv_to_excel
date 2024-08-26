import datetime
import logging
import os
import sys
from logging import Logger
from typing import List, Optional

import pandas as pd
import yaml
from openpyxl import load_workbook


class CustomLogger:
    _LOG_FILE_PATH = os.path.join("log", "test.log")

    def __init__(
        self,
        log_file_path: str = _LOG_FILE_PATH,
        log_level: int = logging.INFO,
    ) -> None:
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(log_level)

        file_handler = logging.FileHandler(log_file_path)
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
        self._processed_count = 0
        self._failed_count = 0
        self._failed_hosts: List[str] = []

    def _is_valid_date(self, input_date: str) -> bool:
        try:
            datetime.datetime.strptime(input_date, self._DATE_FORMAT)
            return True
        except ValueError:
            return False

    def _get_input_date_or_yesterday(self) -> str:
        DATE = 1
        if len(sys.argv) > 1:
            input_date = sys.argv[DATE]

            if self._is_valid_date(input_date):
                return input_date
            else:
                self._logger.error(
                    f"Invalid date specified: {input_date}."
                    " Processing will be aborted."
                )
                sys.exit(1)
        else:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return yesterday.strftime(self._DATE_FORMAT)

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
        if os.path.exists(excel_path):
            self._logger.warning(
                f"Excel file '{excel_path}' already exists."
                " Processing will be aborted."
            )
            sys.exit(1)

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
            self._processed_count += 1
        except Exception as e:
            self._logger.error(f"Failed to read CSV file at {csv_path}: {e}")
            self._logger.info(f"Skipping {host_name} sheet due to error.")
            self._failed_count += 1
            self._failed_hosts.append(host_name)

    def _create_no_data_sheet_to_excel(
        self, writer: pd.ExcelWriter, host_name: str
    ) -> None:
        df_for_not_found = pd.DataFrame({"A": ["No CSV file found."]})
        df_for_not_found.to_excel(
            writer, sheet_name=host_name, index=False, header=False
        )
        self._processed_count += 1

        GRAY = "C0C0C0"
        writer.sheets[host_name].sheet_properties.tabColor = GRAY

        self._logger.info(
            f"Wrote 'No CSV file found.' in cell A1 of '{host_name}' sheet."
        )

    def _add_sheet_for_target(
        self, writer: pd.ExcelWriter, host_folder_path: str, date: str
    ) -> None:
        csv_file_path = self._get_merged_csv_path(host_folder_path, date)
        host_name = os.path.basename(host_folder_path)

        if csv_file_path:
            self._copy_csv_to_excel(writer, csv_file_path, host_name)
        else:
            self._create_no_data_sheet_to_excel(writer, host_name)

    def _folder_name_startswith_target_name(
        self, host_folder: str, targets: List[str]
    ) -> bool:
        return any(host_folder.startswith(target) for target in targets)

    def _search_and_append_csv_to_excel(
        self,
        log_directory: str,
        date: str,
        excel_path: str,
        targets: List[str],
    ) -> None:
        with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a") as writer:
            for host_folder in os.listdir(log_directory):
                if not self._folder_name_startswith_target_name(
                    host_folder, targets
                ):
                    continue

                host_folder_path = os.path.join(log_directory, host_folder)
                if os.path.isdir(host_folder_path):
                    self._add_sheet_for_target(writer, host_folder_path, date)

    def _remove_sentinel_sheet(self, excel_path: str) -> None:
        workbook = load_workbook(excel_path)
        if "SENTINEL_SHEET" in workbook.sheetnames:
            del workbook["SENTINEL_SHEET"]
            workbook.save(excel_path)
            self._logger.info(f"Removed SENTINEL_SHEET from {excel_path}.")
        else:
            self._logger.warning(f"SENTINEL_SHEET not found in {excel_path}.")

    def _log_summary(self) -> None:
        if self._failed_count > 0:
            self._logger.warning(
                f"Processing Summary: {self._processed_count} succeeded,"
                f" {self._failed_count} failed."
            )
            self._logger.warning(
                f"Failed hosts: {', '.join(self._failed_hosts)}"
            )
        else:
            self._logger.info(
                f"Processing Summary: {self._processed_count} succeeded,"
                f" {self._failed_count} failed."
            )

    def main(self) -> None:
        self._logger.info("Process started.")

        date = self._get_input_date_or_yesterday()
        targets = self._get_targets_from_args_or_config()

        self._create_output_folder_for_excel(date)
        file_name_suffix = self._determine_file_name_suffix(targets)
        excel_name = f"{date}_{file_name_suffix}.xlsx"
        excel_path = os.path.join(self._EXCEL_FOLDER_PATH, date, excel_name)

        self._create_excel_with_sentinel_sheet(excel_path)
        self._search_and_append_csv_to_excel(
            self._LOG_FOLDER_PATH, date, excel_path, targets
        )
        self._remove_sentinel_sheet(excel_path)

        self._log_summary()
        self._logger.info("Process completed.")


if __name__ == "__main__":  # pragma: no cover
    consolidator = CSVConsolidator()
    consolidator.main()
