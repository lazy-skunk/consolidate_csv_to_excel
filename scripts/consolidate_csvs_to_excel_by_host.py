import os
import sys

import pandas as pd

from src.config_loader import ConfigLoader
from src.csv_consolidator import CSVConsolidator
from src.csv_path_mapper import CSVPathMapper
from src.custom_logger import CustomLogger
from src.date_handler import DateHandler
from src.excel_analyzer import ExcelAnalyzer
from src.file_utility import FileUtility
from src.processing_summary import ProcessingSummary
from src.target_handler import TargetHandler

_CONFIG_FILE_PATH = os.path.join("config", "config.yml")


def main() -> None:
    try:
        logger = CustomLogger.get_logger()
        logger.info("Process started.")

        date_range = DateHandler.get_date_range_or_yesterday()
        config_loader = ConfigLoader(_CONFIG_FILE_PATH)
        target_prefixes = TargetHandler.get_target_prefixes(config_loader)
        target_fullnames = TargetHandler.get_target_fullnames(target_prefixes)

        targets_with_csv_path_for_each_date = (
            CSVPathMapper.get_csv_path_for_each_date_by_targets(
                date_range, target_fullnames
            )
        )
        processing_time_threshold = (
            config_loader.get_processing_time_threshold()
        )
        processing_summary = ProcessingSummary()
        processing_summary.add_missing_csv_info(
            targets_with_csv_path_for_each_date
        )

        for target_fullname in target_fullnames:
            csv_paths_for_each_date = targets_with_csv_path_for_each_date.get(
                target_fullname
            )

            if csv_paths_for_each_date is None or all(
                csv_path is None
                for csv_path in csv_paths_for_each_date.values()
            ):
                logger.warning(
                    f"No CSV files found for host {target_fullname}."
                )
                continue

            excel_path = FileUtility.create_target_based_excel_path(
                target_fullname
            )
            FileUtility.create_directory(excel_path)

            logger.info(f"Starting to create {excel_path}.")
            with pd.ExcelWriter(
                excel_path, engine="openpyxl", mode="w"
            ) as writer:
                workbook = writer.book

                csv_consolidator = CSVConsolidator(writer, workbook)
                csv_consolidator.consolidate_csvs_to_excel(
                    csv_paths_for_each_date
                )

                excel_analyzer = ExcelAnalyzer(workbook)
                excel_analyzer.highlight_cells_and_sheet_tabs_by_criteria(
                    processing_time_threshold
                )
                excel_analyzer.reorder_sheets_by_color()

                logger.info(f"Saving {excel_path}.")

            processing_summary.save_daily_processing_results(
                target_fullname,
                csv_consolidator,
                excel_analyzer,
            )
            logger.info(f"Finished creating {excel_path}.")

        processing_summary.log_daily_summaries()
        logger.info("Process completed.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
