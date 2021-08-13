import json
import pathlib
from typing import Optional
import logging


def get_file_size_info_str(fp: pathlib.Path) -> str: 
    return convert_size_to_str(fp.stat().st_size)

def convert_size_to_str(file_size:int) -> str:
    file_size = file_size  / 1024 / 1024
    unit = "MB"
    if file_size > 1000:
        file_size = file_size / 1024
        unit = "GB"
    file_size = round(file_size, 2)
    return f"{file_size:,} {unit}"


def count_and_log(n:int) -> int:
    if n % 10000 == 0:
        logging.info(f"{n:,} comments processed so far")
    return n + 1


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