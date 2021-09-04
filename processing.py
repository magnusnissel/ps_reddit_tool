import logging
import json
import datetime
import pathlib
from typing import Optional
from config import DATA_DIR
from helpers import count_and_log, is_json_line, create_ln_str_with_json_boilerplate, get_file_size_info_str


def _split_extracted_at_once(in_fp: pathlib.Path, prefix: str, subreddit: str):
    out_sub_dn = DATA_DIR / f"extracted/daily/{subreddit}"

    data = json.loads(in_fp.read_text())
    cur_day = None
    cur_data = []
    for d in data:
        day = datetime.datetime.utcfromtimestamp(int(d["created_utc"])).date()
        if day != cur_day:
            if len(cur_data) > 0:
                out_fp = out_sub_dn / f"{prefix}_{subreddit}_{cur_day.isoformat()}.json"
                out_fp.write_text(json.dumps(cur_data))
                cur_data = []
            cur_day = day
        cur_data.append(d)
    if len(cur_data) > 0:
        out_fp = out_sub_dn / f"{prefix}_{subreddit}_{cur_day.isoformat()}.json"
        out_fp.write_text(json.dumps(cur_data, indent=4))


def _split_extracted_by_streaming(in_fp: pathlib.Path, prefix: str, subreddit: str):
    out_sub_dn = DATA_DIR / f"extracted/daily/{subreddit}"

    with open(in_fp, mode="r", encoding="utf-8") as h_in:
        h_out = None
        cur_day = None
        for ln in h_in:
            if is_json_line(ln):
                ln = ln.strip().strip(",").strip()  # remove whitespace and trailing comma
                d = json.loads(ln)
                day = datetime.datetime.utcfromtimestamp(int(d["created_utc"])).date()
                if day != cur_day:
                    if h_out is not None:
                        if n > 0:  # write final ]
                            h_out.write("\n]")
                        h_out.close()
                    out_fp = out_sub_dn / f"{prefix}_{subreddit}_{day.isoformat()}.json"
                    n = 0
                    h_out = open(out_fp, mode="w", encoding="utf-8")
                    cur_day = day
                ln_str = create_ln_str_with_json_boilerplate(ln, n)
                h_out.write(ln_str)
                n = count_and_log(n)
        if h_out is not None:
            if n > 0:  # write final ]
                h_out.write("\n]")
            h_out.close()


def split_extracted(prefix: str, year: int, month: int, subreddit: str, stream_threshold: int = 500) -> None:
    """Split extracted subreddit/year/month files further by day"""
    subreddit = subreddit.lower()
    in_sub_dn = DATA_DIR / f"extracted/monthly/{subreddit}"
    out_sub_dn = DATA_DIR / f"extracted/daily/{subreddit}"
    out_sub_dn.mkdir(parents=True, exist_ok=True)
    in_fp = in_sub_dn / f"{prefix}_{subreddit}_{year}-{str(month).zfill(2)}.json"
    split_start = datetime.datetime.utcnow()
    try:
        file_size = in_fp.stat().st_size
    except FileNotFoundError as e:
        print(e)
        logging.error(f"Unable to find file {in_fp} for splitting")
    else:

        logging.info(f"Splitting '{in_fp}' ({get_file_size_info_str(in_fp)}) into daily files")
        file_size = file_size / 1024 / 1024
        # if file size (in MB) is great than stream_threshold (default 500MB), then stream read & write the file(s) line by line
        if file_size > stream_threshold:
            _split_extracted_by_streaming(in_fp, prefix, subreddit)
        else:
            _split_extracted_at_once(in_fp, prefix, subreddit)

        duration = str(datetime.datetime.utcnow() - split_start).split(".")[0].zfill(8)
        logging.info(f"Splitting process completed after {duration}")
