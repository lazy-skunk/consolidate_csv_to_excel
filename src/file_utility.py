import os

_EXCEL_FOLDER_PATH = os.path.join("output")


class FileUtility:
    @staticmethod
    def create_target_based_excel_path(target_fullname: str) -> str:
        excel_name = f"{target_fullname}.xlsx"
        excel_path = os.path.join(_EXCEL_FOLDER_PATH, excel_name)
        return excel_path

    @staticmethod
    def create_date_based_excel_path(date: str, suffix: str) -> str:
        excel_name = f"{date}_{suffix}.xlsx"
        excel_path = os.path.join(_EXCEL_FOLDER_PATH, date, excel_name)
        return excel_path

    @staticmethod
    def create_directory(file_path: str) -> None:
        directory_for_file = os.path.dirname(file_path)
        os.makedirs(directory_for_file, exist_ok=True)

    @staticmethod
    def get_csv_path(target_folder_path: str, date: str) -> str | None:
        csv_name = f"test_{date}.csv"
        csv_path = os.path.join(target_folder_path, csv_name)

        if os.path.exists(csv_path):
            return csv_path
        else:
            return None
