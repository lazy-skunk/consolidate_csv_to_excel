import os
import sys
from typing import List

from src.config_loader import ConfigLoader

_TARGET_FOLDERS_BASE_PATH = os.path.join("log_directory")


class TargetHandler:
    @classmethod
    def get_target_prefixes(cls, config_loader: ConfigLoader) -> List[str]:
        TARGET_INDEX = 2
        if len(sys.argv) > TARGET_INDEX:
            targets = sys.argv[TARGET_INDEX].split(",")
        else:
            targets = config_loader.get("targets", [])
        return targets

    @classmethod
    def get_target_fullnames(cls, target_prefixes: List[str]) -> List[str]:
        target_fullnames = []
        target_folders = os.listdir(_TARGET_FOLDERS_BASE_PATH)

        for target_prefix in target_prefixes:
            matched_target_fullnames = [
                target_folder
                for target_folder in target_folders
                if target_folder.startswith(target_prefix)
            ]

            if matched_target_fullnames:
                target_fullnames.extend(matched_target_fullnames)
            else:
                raise ValueError(
                    f"No folder starting with target prefix '{target_prefix}'"
                    " was found in the log directory."
                )

        return target_fullnames
