import logging
import json
import datetime
from typing import Optional

from helpers import determine_data_dir, count_and_log


def split_extracted(year:int, month:int, subreddit:str, delete_source:bool=False, folder:Optional[str]=None) -> None:
    """Split extracted subreddit/year/month files further by day"""
    data_dir = determine_data_dir(folder)
    subreddit = subreddit.lower()
    dn = data_dir / subreddit
    in_fp = dn / f"{subreddit}_{year}-{str(month).zfill(2)}"
    split_start = datetime.datetime.utcnow()
    logging.info(f"Splitting '{in_fp}' into daily files")
    try:
        with open(in_fp, mode="r", encoding="utf-8") as h_in:
            h_out = None
            cur_day = None
            n = 1
            for ln in h_in:
                d = json.loads(ln)
                day = datetime.datetime.utcfromtimestamp(int(d["created_utc"])).date()
                if day != cur_day:
                    if h_out is not None:
                        h_out.close()
                    out_fp = dn / f"{subreddit}_{day.isoformat()}"
                    h_out = open(out_fp, mode="w", encoding="utf-8")
                    cur_day = day
                    logging.info(f"Splitting comments to {out_fp}")
                h_out.write(ln)
                n = count_and_log(n)
            if h_out is not None:
                h_out.close()
    except FileNotFoundError as e:
        logging.error(f"Unable to find file {in_fp} for splitting")
    else:
        if delete_source is True:
            in_fp.unlink()
            logging.info(f"Deleted {in_fp} after splitting into smaller daily files")
    duration = str(datetime.datetime.utcnow() - split_start).split(".")[0].zfill(8)
    logging.info(f"Splitting process completed after {duration}")