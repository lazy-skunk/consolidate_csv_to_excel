import datetime
import sys

import yaml


def get_date_from_args_or_default() -> str:
    """引数から日付を取得するか、引数がない場合は昨日の日付を返す"""
    _DATE = 1
    if len(sys.argv) > 1:
        return sys.argv[_DATE]
    else:
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        return yesterday.strftime("%Y%m%d")


def get_targets_from_args_or_config() -> str:
    """引数からターゲットを取得するか、引数がない場合は config.yml から取得する"""
    _TARGETS = 2
    if len(sys.argv) > 2:
        return sys.argv[_TARGETS]
    else:
        with open("src/config.yml", "r") as file:
            config = yaml.safe_load(file)
            return ",".join(config.get("targets", []))


def args_check(arg1: str | None, arg2: str | None) -> None:
    print(f"arg1: {arg1}")
    print(f"arg2: {arg2}")


if __name__ == "__main__":
    date = get_date_from_args_or_default()
    targets = get_targets_from_args_or_config()

    args_check(date, targets)
