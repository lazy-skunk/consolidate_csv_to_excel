import os
from typing import Dict, List

from src.file_utility import FileUtility

_TARGET_FOLDERS_BASE_PATH = os.path.join("log_directory")


class CSVPathMapper:
    @staticmethod
    def get_targets_and_csv_paths_by_dates(
        date_range: List[str], target_fullnames: List[str]
    ) -> Dict[str, Dict[str, str | None]]:
        targets_and_csv_paths_by_dates = {}

        for date in date_range:
            targets_and_csv_paths = {
                target_fullname: FileUtility.get_csv_path(
                    os.path.join(_TARGET_FOLDERS_BASE_PATH, target_fullname),
                    date,
                )
                for target_fullname in target_fullnames
            }

            targets_and_csv_paths_by_dates[date] = targets_and_csv_paths

        return targets_and_csv_paths_by_dates

    @staticmethod
    def get_csv_path_for_each_date_by_targets(
        date_range: List[str], target_fullnames: List[str]
    ) -> Dict[str, Dict[str, str | None]]:
        csv_path_for_each_date_by_targets = {}

        for target_fullname in target_fullnames:
            csv_path_for_each_date = {
                date: FileUtility.get_csv_path(
                    os.path.join(_TARGET_FOLDERS_BASE_PATH, target_fullname),
                    date,
                )
                for date in date_range
            }
            csv_path_for_each_date_by_targets[target_fullname] = (
                csv_path_for_each_date
            )

        return csv_path_for_each_date_by_targets
