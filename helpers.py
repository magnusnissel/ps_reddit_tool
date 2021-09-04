import json
from json.decoder import JSONDecodeError
import pathlib
from typing import Optional
import logging
import abc
import datetime
from typing import Union, Tuple, Optional


class AbstractTool(abc.ABC):
    def __init__(self):
        self.since, self.until = {}, {}
        self.periods = []

    @staticmethod
    def _interpret_date_input(date_str: str) -> dict:
        d = {}
        try:
            dt = datetime.datetime.strptime(date_str, "%Y-%m")
        except ValueError:
            try:
                dt = datetime.datetime.strptime(date_str, "%Y")
            except ValueError as e:
                raise (e)
            else:
                d = {"year": dt.year, "month": None}
        else:
            d = {"year": dt.year, "month": dt.month}
        return d

    def _set_periods(self) -> list:
        years = [y for y in range(self.since["year"], self.until["year"] + 1)]
        self.periods = []
        for y in years:
            if y not in (
                self.since["year"],
                self.until["year"],
            ):  # for any years in between always get months 1-12
                months = [m for m in range(1, 13)]
            elif self.since["year"] == self.until["year"]:
                months = [m for m in range(self.since["month"], self.until["month"] + 1)]
            elif y == self.since["year"]:
                months = [m for m in range(self.since["month"], 13)]
            elif y == self.until["year"]:
                months = [m for m in range(1, self.until["month"] + 1)]
            self.periods += [(y, m) for m in months]

    def _interpret_date_range(self, since: Union[str, int], until: Union[str, int, None]) -> Tuple[dict]:
        since = self._interpret_date_input(str(since))  #  if only a year as given, Fire interprets is as int
        if until is None:
            until = since
        else:
            until = self._interpret_date_input(str(until))
        # If no month specified, assume from January of the first year until December of the last year
        if since["month"] is None:
            since["month"] = 1
        if until["month"] is None:
            until["month"] = 12
        self.since = since
        self.until = until

    def _initialize_dates(self, since: Union[str, int], until: Union[str, int, None]):
        self._interpret_date_range(since, until)
        self._set_periods()

    def _get_date_range_str(self, sep: str = "to"):
        return f"{self.periods[0][0]}-{str(self.periods[0][1]).zfill(2)} {sep} {self.periods[-1][0]}-{str(self.periods[-1][1]).zfill(2)}"


def get_file_size_info_str(fp: pathlib.Path) -> str:
    return convert_size_to_str(fp.stat().st_size)


def convert_size_to_str(file_size: int) -> str:
    file_size = file_size / 1024 / 1024
    unit = "MB"
    if file_size > 1000:
        file_size = file_size / 1024
        unit = "GB"
    file_size = round(file_size, 2)
    return f"{file_size:,} {unit}"


def count_and_log(n: int) -> int:
    n += 1
    if n % 10000 == 0:
        logging.info(f"{n:,} lines extracted so far")
    return n


def infer_extension(prefix: str, year: int, month: int) -> str:
    ext = "zst"
    if prefix == "RS":
        return ext
    else:
        if (year == 2018 and month < 10) or (year == 2017 and month == 12):
            ext = "xz"
        elif year < 2017 or (year == 2017 and month < 12):
            ext = "bz2"
        return ext


def is_relevant_ln(ln: str, subreddit: str) -> bool:
    if len(ln.strip()) > 0:
        try:
            d = json.loads(ln)
        except JSONDecodeError as e:
            logging.error("JSON DECODE ERROR")
            logging.error(e)
            logging.warning(ln)
        else:
            return d["subreddit"].lower() == subreddit
    else:
        return False


def create_ln_str_with_json_boilerplate(ln: str, n: int) -> str:
    parts = []
    if n == 0:
        parts.append("[\n")  # write initial [ for array of json obj
    else:
        parts.append(",\n")  # write comma and new line before adding next
    parts.append(ln)
    return "".join(parts)


def is_json_line(ln: str) -> bool:
    if len(ln.strip()) == 0:  # empty line
        return False
    elif ln[0] in {"[", "]"}:  # start / end of array
        return False
    return True
