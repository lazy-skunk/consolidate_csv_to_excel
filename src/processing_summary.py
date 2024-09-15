from typing import Dict, List, Set

from src.csv_consolidator import CSVConsolidator
from src.custom_logger import CustomLogger
from src.excel_analyzer import ExcelAnalyzer


class ProcessingSummary:
    _logger = CustomLogger.get_logger()

    def __init__(self) -> None:
        self.daily_summaries: Dict[str, List[str]] = {}
        self.daily_processing_results: Dict[str, Dict[str, Set[str]]] = {}

    def add_missing_csv_info(
        self,
        targets_and_csv_path_by_dates: Dict[str, Dict[str, str | None]],
    ) -> None:
        for (
            date,
            targets_and_csv_path,
        ) in targets_and_csv_path_by_dates.items():
            if all(
                csv_path is None for csv_path in targets_and_csv_path.values()
            ):
                self.daily_summaries.setdefault(date, []).append(
                    "No CSV files found."
                )
            else:
                missing_targets = [
                    target_fullname
                    for target_fullname, csv_path in targets_and_csv_path.items()  # noqa E501
                    if csv_path is None
                ]
                if missing_targets:
                    self.daily_summaries.setdefault(date, []).append(
                        f"Partial data loss : {missing_targets}"
                    )

    def save_daily_processing_results(
        self,
        key: str,
        csv_consolidator: CSVConsolidator,
        excel_analyzer: ExcelAnalyzer,
    ) -> None:
        merge_failed_info = csv_consolidator.get_merge_failed_info()
        analysis_results = excel_analyzer.get_analysis_results()

        self.daily_processing_results.setdefault(
            key,
            {
                "merge_failed": set(),
                "threshold_exceeded": set(),
                "anomaly_detected": set(),
            },
        )

        self.daily_processing_results[key]["merge_failed"].update(
            merge_failed_info["merge_failed"]
        )

        self.daily_processing_results[key]["threshold_exceeded"].update(
            analysis_results["threshold_exceeded"]
        )

        self.daily_processing_results[key]["anomaly_detected"].update(
            analysis_results["anomaly_detected"]
        )

    def _summarize_daily_processing_results(self) -> None:
        for date, summary in self.daily_processing_results.items():
            day_summary = []

            if summary.get("threshold_exceeded"):
                threshold_exceeded = summary["threshold_exceeded"]
                day_summary.append(
                    "Exceeded threshold detected :" f" {threshold_exceeded}"
                )

            if summary.get("anomaly_detected"):
                anomaly_detected = summary["anomaly_detected"]
                day_summary.append(
                    "Anomaly value detected :" f" {anomaly_detected}"
                )

            if summary.get("merge_failed"):
                merge_failed = summary["merge_failed"]
                day_summary.append(f"Merge failed sheets : {merge_failed}")

            self.daily_summaries.setdefault(date, []).extend(day_summary)

    def log_daily_summaries(self) -> None:
        self._logger.info("Starting to log summary.")
        self._summarize_daily_processing_results()

        for date in sorted(self.daily_summaries.keys()):
            self._logger.info(f"Summary for {date}:")

            if not self.daily_summaries[date]:
                self._logger.info("No anomalies detected.")
            else:
                for summary_item in self.daily_summaries[date]:
                    self._logger.warning(f"{summary_item}")

        self._logger.info("Finished logging summary.")
