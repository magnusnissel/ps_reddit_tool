
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
    in_dn = determine_data_dir(folder, "compressed")
    out_dn = determine_data_dir(folder, f"extracted/monthly/{subreddit}")

    ext = infer_extension(year, month)
    subreddit = subreddit.lower()
    date_str = f"{year}-{str(month).zfill(2)}"
    out_fp = out_dn / f"{subreddit}_{date_str}"
    if force is True or not out_fp.is_file():
        ext_start = datetime.datetime.utcnow()

        if year < 2020:
            fn = f"RC_{date_str}.{ext}"
            files = [in_dn / fn]
        else:
            files = []
            for d in range(1, 32):
                try:
                    day = datetime.date(year=year, month=month, day=d)
                except ValueError: #invalid date, e.g. February 30th
                    pass
                else:
                    fn = f"RC_{day.isoformat()}.{ext}"
                    fp = in_dn / fn
                    files.append(fp)
        for fp in files:
            if fp.is_file():
                logging.info(f"Extracting comments for subreddit '{subreddit}' from {fp} to {out_fp}")

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
