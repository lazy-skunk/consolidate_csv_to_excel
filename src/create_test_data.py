import json
import random
from datetime import datetime, timedelta

import pandas as pd

_BASE_DATE = datetime.now()
_data = []

for i in range(200000):
    date_a = _BASE_DATE - timedelta(seconds=i)
    date_b = _BASE_DATE + timedelta(seconds=i)

    time_difference = (date_b - date_a).total_seconds()

    random_value = random.choice([None, True])
    json_list = []
    for j in range(2):
        json_data = {
            "date_a": date_a.isoformat(),
            "date_b": date_b.isoformat(),
            "time_difference": time_difference,
            "random_key": random_value,
        }
        json_list.append(json_data)

    stringified_json = json.dumps(json_list)

    _data.append(
        [
            date_a.strftime("%Y-%m-%d %H:%M:%S"),
            date_b.strftime("%Y-%m-%d %H:%M:%S"),
            time_difference,
            stringified_json,
        ]
    )

_df = pd.DataFrame(
    _data, columns=["Date A", "Date B", "Time Difference", "JSON"]
)

_df.to_csv("test_20380101.csv", index=False)
