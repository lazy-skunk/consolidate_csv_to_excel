import json
import random
from datetime import datetime, timedelta

import pandas as pd


def create_test_csv() -> None:
    BASE_DATE = datetime.now()
    data = []

    for i in range(10000):
        date_a = BASE_DATE - timedelta(seconds=i)
        date_b = BASE_DATE + timedelta(seconds=i)
        processing_time = int((date_b - date_a).total_seconds())
        random_value = random.choice([None, True])

        json_list = []
        json_data = {
            "date_a": date_a.isoformat(),
            "date_b": date_b.isoformat(),
            "processing_time": f"{processing_time}s",
            "random_key": random_value,
        }
        for _ in range(2):
            json_list.append(json_data)

        stringified_json = json.dumps(json_list)

        data.append(
            [
                date_a.strftime("%Y-%m-%d %H:%M:%S"),
                date_b.strftime("%Y-%m-%d %H:%M:%S"),
                f"{processing_time}s",
                stringified_json,
            ]
        )

    df = pd.DataFrame(
        data, columns=["Date_A", "Date_B", "Processing_Time", "JSON"]
    )

    df.to_csv("test_20380101.csv", index=False)


if __name__ == "__main__":
    create_test_csv()
