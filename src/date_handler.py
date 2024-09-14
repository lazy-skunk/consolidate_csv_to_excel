import datetime
import sys
from typing import List


class DateHandler:
    _DATE_FORMAT = "%Y%m%d"
    _DATE_DELIMITER = "~"
    _DATE_LENGTH = 8

    @classmethod
    def _parse_date(cls, input_date: str) -> datetime.datetime:
        if len(input_date) != cls._DATE_LENGTH or not input_date.isdigit():
            raise ValueError(
                f"Date must be {cls._DATE_LENGTH} digits in YYYYMMDD format."
                " For a date range, please use the format YYYYMMDD~YYYYMMDD."
                f" Input value : {input_date}"
            )

        date = datetime.datetime.strptime(input_date, DateHandler._DATE_FORMAT)

        if date > datetime.datetime.now():
            raise ValueError(
                f"Future date specified. Input value : {input_date}"
            )

        return date

    @classmethod
    def _generate_date_range(
        cls, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> List[str]:
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        current_date = start_date
        date_list = []
        while current_date <= end_date:
            date_list.append(current_date.strftime(cls._DATE_FORMAT))
            current_date += datetime.timedelta(days=1)

        return date_list

    @classmethod
    def get_date_range_or_yesterday(cls) -> List[str]:
        DATE_INDEX = 1

        if len(sys.argv) <= DATE_INDEX:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return [yesterday.strftime(cls._DATE_FORMAT)]

        input_date = sys.argv[DATE_INDEX]

        if cls._DATE_DELIMITER in input_date:
            start_date_str, end_date_str = input_date.split(
                cls._DATE_DELIMITER
            )

            start_date = cls._parse_date(start_date_str)
            end_date = cls._parse_date(end_date_str)

            return cls._generate_date_range(start_date, end_date)

        date = cls._parse_date(input_date)
        return [date.strftime(cls._DATE_FORMAT)]
