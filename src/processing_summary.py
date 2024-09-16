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
        data_by_base_key: Dict[str, Dict[str, str | None]],
    ) -> None:
        for (
            base_key,
            sub_keys_and_csv_path,
        ) in data_by_base_key.items():
            if all(
                csv_path is None for csv_path in sub_keys_and_csv_path.values()
            ):
                self.daily_summaries.setdefault(base_key, []).append(
                    "No CSV files found."
                )
            else:
                sub_keys_without_csv = [
                    sub_key
                    for sub_key, csv_path in sub_keys_and_csv_path.items()
                    if csv_path is None
                ]
                if sub_keys_without_csv:
                    self.daily_summaries.setdefault(base_key, []).append(
                        f"Some CSV files not found: {sub_keys_without_csv}"
                    )

    def save_daily_processing_results(
        self,
        dict_key: str,
        csv_consolidator: CSVConsolidator,
        excel_analyzer: ExcelAnalyzer,
    ) -> None:
        merge_failed_info = csv_consolidator.get_merge_failed_info()
        analysis_results = excel_analyzer.get_analysis_results()

        self.daily_processing_results.setdefault(
            dict_key,
            {
                "merge_failed": set(),
                "threshold_exceeded": set(),
                "anomaly_detected": set(),
            },
        )

        self.daily_processing_results[dict_key]["merge_failed"].update(
            merge_failed_info["merge_failed"]
        )

        self.daily_processing_results[dict_key]["threshold_exceeded"].update(
            analysis_results["threshold_exceeded"]
        )

        self.daily_processing_results[dict_key]["anomaly_detected"].update(
            analysis_results["anomaly_detected"]
        )

    def _summarize_daily_processing_results(self) -> None:
        for date, summary in self.daily_processing_results.items():
            day_summary = []

            if summary.get("threshold_exceeded"):
                threshold_exceeded = summary["threshold_exceeded"]
                day_summary.append(
                    f"Exceeded threshold detected: {threshold_exceeded}"
                )

            if summary.get("anomaly_detected"):
                anomaly_detected = summary["anomaly_detected"]
                day_summary.append(
                    f"Anomaly value detected: {anomaly_detected}"
                )

            if summary.get("merge_failed"):
                merge_failed = summary["merge_failed"]
                day_summary.append(f"Merge failed sheets: {merge_failed}")

            self.daily_summaries.setdefault(date, []).extend(day_summary)

    def log_daily_summaries(self) -> None:
        self._logger.info("Starting to log summary.")
        self._summarize_daily_processing_results()

        for key in sorted(self.daily_summaries.keys()):
            self._logger.info(f"Summary for {key}:")

            if not self.daily_summaries[key]:
                self._logger.info("No anomalies detected.")
            else:
                for summary_item in self.daily_summaries[key]:
                    self._logger.warning(f"{summary_item}")

        self._logger.info("Finished logging summary.")
