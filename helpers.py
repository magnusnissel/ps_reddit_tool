import json
import pathlib
from config import DEFAULT_DATA_DIR
from typing import Optional
import logging


def count_and_log(n:int) -> int:
    if n % 10000 == 0:
        logging.info(f"{n:,} comments processed so far")
    return n + 1

def determine_data_dir(folder:Optional[str]=None) -> pathlib.Path:
    """Utility function to check if a folder is provided or if the default should be used, also creates the dir if not existing"""
    if folder is not None:
        data_dir = pathlib.Path(folder)
    else:
        data_dir = DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def infer_extension(year:int, month:int) -> str:
    ext = "zst"
    if (year == 2018 and month < 10) or (year == 2017 and month == 12):
        ext = "xz"
    elif year < 2017 or (year == 2017 and month < 12):
        ext = "bz2"
    return ext


def is_relevant_ln(ln:str, subreddit:str) -> bool:
    d = json.loads(ln.decode("utf-8"))
    return d["subreddit"].lower() == subreddit