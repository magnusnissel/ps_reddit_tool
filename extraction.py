import datetime
import logging
from typing import Optional
import bz2
import lzma
import json
import zstandard
from config import DATA_DIR
from helpers import (
    infer_extension,
    is_relevant_ln,
    count_and_log,
    create_ln_str_with_json_boilerplate,
)


def extract_from_dump(prefix: str, year: int, month: int, subreddit: str, force: bool = False) -> None:
    """Extract json objects for a specific subreddit for a given year and month into a single year/month file,
    assuming the necessary dump files were downloaded beforehand"""
    in_dn = DATA_DIR / "compressed"
    out_dn = DATA_DIR / f"extracted/monthly/{subreddit}"
    out_dn.mkdir(parents=True, exist_ok=True)
    ext = infer_extension(prefix, year, month)
    n = 0
    if prefix == "RC":
        kind = "comments"
    elif prefix == "RS":
        kind = "submissions"
    subreddit = subreddit.lower()
    date_str = f"{year}-{str(month).zfill(2)}"
    out_fp = out_dn / f"{prefix}_{subreddit}_{date_str}.json"
    if force is True or not out_fp.is_file():
        ext_start = datetime.datetime.utcnow()
        fn = f"{prefix}_{date_str}.{ext}"
        files = [in_dn / fn]
        for fp in files:
            if fp.is_file():

                logging.info(f"Extracting {kind} for subreddit '{subreddit}' from {fp} to {out_fp}")

                with open(out_fp, mode="w", encoding="utf-8") as h_out:
                    n = 0
                    if ext == "bz2":
                        with bz2.BZ2File(fp) as h_in:
                            for ln in h_in:
                                ln = ln.decode("utf-8")
                                if is_relevant_ln(ln, subreddit) is True:
                                    ln_str = create_ln_str_with_json_boilerplate(ln, n)
                                    h_out.write(ln_str)
                                    n = count_and_log(n)
                        if n > 0:  # write final ]
                            h_out.write("\n]")
                    elif ext == "xz":
                        with lzma.LZMAFile(fp) as h_in:
                            for ln in h_in:
                                ln = ln.decode("utf-8")
                                if is_relevant_ln(ln, subreddit) is True:
                                    ln_str = create_ln_str_with_json_boilerplate(ln, n)
                                    h_out.write(ln_str)
                                    n = count_and_log(n)
                        if n > 0:  # write final ]
                            h_out.write("\n]")
                    elif ext == "zst":

                        chunksize = (
                            2 ** 23
                        )  # 8MB per chunk to reduce the immpact of "unexpected end of data" errors until fixed
                        with open(fp, "rb") as h_in:
                            decomp = zstandard.ZstdDecompressor(max_window_size=2147483648)

                            with decomp.stream_reader(h_in) as reader:
                                prev_ln = ""
                                while True:
                                    chunk = reader.read(chunksize)
                                    if not chunk:
                                        break
                                    try:
                                        lines = chunk.decode("utf-8").split("\n")
                                    except UnicodeDecodeError as e:
                                        logging.warning(e)
                                        # Attempt to ignore that segment
                                        chunk = chunk[: e.start] + chunk[e.end :]
                                        try:
                                            lines = chunk.decode("utf-8").split("\n")
                                        except UnicodeDecodeError as e:
                                            logging.error(e)
                                            lines = []

                                    for i, ln in enumerate(lines[:-1]):
                                        if i == 0:
                                            ln = f"{prev_ln}{ln}"
                                            ln = ln.strip()
                                        if is_relevant_ln(ln, subreddit) is True:
                                            ln_str = create_ln_str_with_json_boilerplate(ln, n)
                                            h_out.write(ln_str)
                                            n = count_and_log(n)
                                        prev_ln = lines[-1]
                                if n > 0:  # write final ]
                                    h_out.write("\n]")
                if n > 0:
                    logging.info(f"Saved {n:,} lines to {out_fp}")
                else:
                    try:
                        out_fp.unlink()
                    except FileNotFoundError:
                        pass
            else:
                logging.warning(f"File {fp} not found for extraction")
        duration = str(datetime.datetime.utcnow() - ext_start).split(".")[0].zfill(8)
        logging.info(f"Extraction process of {n} lines completed after {duration}")
    else:
        logging.info(
            f"Skipping extraction to {out_fp} because the file already exists  (--force=True to override this)"
        )
