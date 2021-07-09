import pathlib
import datetime
import shutil
import logging
import bz2
import lzma
import json
from typing import Optional

from urllib3.util.retry import Retry
import urllib3
import pyzstd
import fire

from config import DEFAULT_DATA_DIR


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler("ps_dump_extractor.log"),
                              logging.StreamHandler()])


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


def count_and_log(n:int) -> int:
    if n % 10000 == 0:
        logging.info(f"{n:,} comments extracted so far")
    return n + 1


def extract_from_dump(subreddit:str, year:int, month:int, folder:Optional[str]=None) -> None:
    if folder is not None:
        data_dir = pathlib.Path(folder)
    else:
        data_dir = DEFAULT_DATA_DIR
    ext = infer_extension(year, month)
    subreddit = subreddit.lower()
    dn = data_dir / subreddit
    dn.mkdir(exist_ok=True, parents=True)
    date_str = f"{year}-{str(month).zfill(2)}"
    out_fp = dn / f"{subreddit}_{date_str}"
    if year < 2020:
        fn = f"RC_{date_str}.{ext}"
        files = [data_dir / fn]
    logging.info(f"Extracting comments for subreddit '{subreddit}' from {len(files)} file(s)")
    for fp in files:
        if fp.is_file():
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
            logging.info(f"Saved {n:} comments to {out_fp}")




def download_file(url:str, filepath:pathlib.Path) -> bool:
    retries = Retry(connect=5, read=3, redirect=3)
    http = urllib3.PoolManager(retries=retries)
    try:
        with http.request('GET',url, preload_content=False) as resp, open(filepath, 'wb') as outpath:
            shutil.copyfileobj(resp, outpath)
        return True
    except urllib3.exceptions.HTTPError as e:
        logging.error(e)
        return False



def download_dump(year:int, month:int, folder:Optional[str]=None) -> None:
    if folder is not None:
        data_dir = pathlib.Path(folder)
    else:

        data_dir = DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)
    ext = infer_extension(year, month)
    if year < 2020:  # monthly archive files, varying extenions
        date_str = f"{year}-{str(month).zfill(2)}"
        urls =[f"https://files.pushshift.io/reddit/comments/RC_{date_str}.{ext}"]
    else:  # daily archive files, zst 
        urls = []
        for d in range(1, 32):
            try:
                day = datetime.date(year=year, month=month, day=d)
            except ValueError: #invalid date, e.g. February 30th
                pass
            else:
                urls.append(f"https://files.pushshift.io/reddit/comments/RC_{day.isoformat()}.{ext}")
    logging.info(f"Downloading {len(urls)} file(s) to {data_dir}")
    for url in urls:
        fn = url.split("/")[-1]
        fp = data_dir / fn
        logging.info(f"Downloading {fn}")
        success = download_file(url, fp)
        if success is True:
            logging.info(f"Downloaded {fn}")
        else:
            logging.warning(f"Did not download {fn}")


    

if __name__ == "__main__":
    fire.Fire({
        'download': download_dump,
        'extract': extract_from_dump,
    })