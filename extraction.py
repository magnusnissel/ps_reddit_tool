
import datetime
import logging
from typing import Optional

import bz2
import lzma
import pyzstd

from helpers import determine_data_dir, infer_extension, is_relevant_ln, count_and_log


def extract_from_dump(year:int, month:int, subreddit:str, force:bool=False, folder:Optional[str]=None) -> None:
    """Extract json objects for a specific subreddit for a given year and month into a single year/month file,
       assuming the necessary dump files were downloaded beforehand"""
    data_dir = determine_data_dir(folder)
    ext = infer_extension(year, month)
    subreddit = subreddit.lower()
    dn = data_dir / subreddit
    dn.mkdir(exist_ok=True, parents=True)
    date_str = f"{year}-{str(month).zfill(2)}"
    out_fp = dn / f"{subreddit}_{date_str}"
    if force is True or not out_fp.is_file():
        ext_start = datetime.datetime.utcnow()

        if year < 2020:
            fn = f"RC_{date_str}.{ext}"
            files = [data_dir / fn]
        else:
            files = []
            for d in range(1, 32):
                try:
                    day = datetime.date(year=year, month=month, day=d)
                except ValueError: #invalid date, e.g. February 30th
                    pass
                else:
                    fn = f"RC_{day.isoformat()}.{ext}"
                    fp = data_dir / fn
                    files.append(fp)
        for fp in files:
            if fp.is_file():
                logging.info(f"Extracting comments for subreddit '{subreddit}' from {fp.name} to {out_fp.name}")

                with open(out_fp, mode="w", encoding="utf-8") as h_out:
                    n = 1
                    if ext == "bz2":
                        with bz2.BZ2File(fp) as h_in:
                            for ln in h_in:
                                if is_relevant_ln(ln, subreddit) is True:
                                    h_out.write(ln.decode("utf-8"))
                                    n = count_and_log(n)
                    elif ext == "xz":
                        with lzma.LZMAFile(fp) as h_in:
                            for ln in h_in:
                                if is_relevant_ln(ln, subreddit) is True:
                                    h_out.write(ln.decode("utf-8"))
                                    n = count_and_log(n)
                    elif ext == "zst":
                        with pyzstd.ZstdFile(fp) as h_in:
                            for ln in h_in:
                                if is_relevant_ln(ln, subreddit) is True:
                                    h_out.write(ln.decode("utf-8"))
                                    n = count_and_log(n)
                logging.info(f"Saved {n:,} comments to {out_fp}")
            else:
                logging.warning(f"File {fp} not found for extraction")
        duration = str(datetime.datetime.utcnow() - ext_start).split(".")[0].zfill(8)
        logging.info(f"Extraction process completed after {duration}")
    else:
        logging.info(f"Skipping comment extraction to {out_fp} because the file already exists  (--force=True to override this)")
   


def batch_extract_from_dumps(from_year: int, to_year:int, subreddit:str, force:bool=False, folder:Optional[str]=None) -> None:
    if from_year > to_year:
        from_year, to_year = to_year, from_year
    batch_start = datetime.datetime.utcnow()
    for y in range(from_year, to_year+1):
        if y > 2005:
            for m in range(1, 13):
                extract_from_dump(year=y, month=m, subreddit=subreddit, force=force, folder=folder)
        else:
            if y == 2005:
                extract_from_dump(year=y, month=12, subreddit=subreddit, force=force, folder=folder)
            else:
                logging.warning(f"No data available for {y}")
    duration = str(datetime.datetime.utcnow() - batch_start).split(".")[0].zfill(8)
    logging.info(f"Batch  process completed after {duration}")