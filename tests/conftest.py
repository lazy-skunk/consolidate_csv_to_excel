import datetime
import os
import tempfile
from typing import Generator

import pytest

_TODAY: str = (datetime.datetime.now()).strftime("%Y%m%d")


@pytest.fixture(scope="function")
def config_path() -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile("w", delete=False) as config_file:
        config_path = config_file.name
        config_content = """
targets:
  - host1
  - host2
"""
        config_file.write(config_content)

    yield config_path

    os.remove(config_path)


@pytest.fixture(scope="function")
def temp_excel_folder() -> Generator[str, None, None]:
    temp_dir = tempfile.mkdtemp()

    yield temp_dir

    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)


@pytest.fixture(scope="function")
def temp_excel_path() -> Generator[str, None, None]:
    excel_path = tempfile.mktemp(suffix=".xlsx")

    yield excel_path

    if os.path.exists(excel_path):
        os.remove(excel_path)


@pytest.fixture(scope="function")
def generated_excel_path() -> Generator[str, None, None]:
    generated_excel = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    generated_excel_path = generated_excel.name
    generated_excel.close()

    yield generated_excel_path

    if os.path.exists(generated_excel_path):
        os.remove(generated_excel_path)


@pytest.fixture(scope="function")
def temp_dir_path_with_today_csv() -> Generator[str, None, None]:
    temp_dir = tempfile.mkdtemp()

    csv_name = f"test_{_TODAY}.csv"
    csv_path = os.path.join(temp_dir, csv_name)

    with open(csv_path, "w") as f:
        f.write("col1,col2\nrow1,row2")

    yield temp_dir

    if os.path.exists(csv_path):
        os.remove(csv_path)
    os.rmdir(temp_dir)
