import pathlib
import datetime
import shutil
import logging
import bz2
import lzma
import json
from typing import Optional
from fire.console import encoding

from urllib3.util.retry import Retry
import urllib3
import pyzstd
import fire

from helpers import infer_extension, is_relevant_ln
from config import DEFAULT_DATA_DIR


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler("ps_dump_extractor.log"),
                              logging.StreamHandler()])


def count_and_log(n:int) -> int:
    if n % 10000 == 0:
        logging.info(f"{n:,} comments processed so far")
    return n + 1



def split_extracted(year:int, month:int, subreddit:str, delete_source:bool=False, folder:Optional[str]=None) -> None:
    """Split extracted subreddit/year/month files further by day"""
    data_dir = determine_data_dir(folder)

    subreddit = subreddit.lower()
    dn = data_dir / "daily_splits" / subreddit
    in_fp = dn / f"{subreddit}_{year}-{str(month).zfill(2)}"
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
    

def extract_from_dump(year:int, month:int, subreddit:str, force:bool=False, folder:Optional[str]=None) -> None:
    """Extract json objects for a specific subreddit for a given year and month into a single year/month file,
       assuninng the necessary dump files were downloaded beforehand"""
    in_data_dir = determine_data_dir(folder, "compressed")
    out_data_dir = determine_data_dir(folder, "extracted")

    ext = infer_extension(year, month)
    subreddit = subreddit.lower()
    subreddit_dn = out_data_dir / subreddit
    subreddit_dn.mkdir(exist_ok=True, parents=True)
    date_str = f"{year}-{str(month).zfill(2)}"
    out_fp = subreddit_dn / f"{subreddit}_{date_str}"
    if force is True or not out_fp.is_file():
        if year < 2020:
            fn = f"RC_{date_str}.{ext}"
            files = [in_data_dir / fn]
        else:
            files = []
            for d in range(1, 32):
                try:
                    day = datetime.date(year=year, month=month, day=d)
                except ValueError: #invalid date, e.g. February 30th
                    pass
                else:
                    fn = f"RC_{day.isoformat()}.{ext}"
                    fp = in_data_dir / fn
                    files.append(fp)
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
                logging.info(f"Saved {n:,} comments to {out_fp}")
            else:
                logging.warning(f"File {fp} not found for extraction")
    else:
        logging.info(f"File {out_fp} already exists, not extracting again unless asked via --force")


def batch_extract_from_dumps(from_year: int, to_year:int, subreddit:str, force:bool=False, folder:Optional[str]=None) -> None:
    if from_year > to_year:
        from_year, to_year = to_year, from_year
    for y in range(from_year, to_year+1):
        if y > 2005:
            for m in range(1, 13):
                download_dump(year=y, month=m, subreddit=subreddit, force=force, folder=folder)
        else:
            if y == 2005:
                download_dump(year=y, month=12, subreddit=subreddit, force=force, folder=folder)
            else:
                logging.warning(f"No data available for {y}")


def download_file(url:str, filepath:pathlib.Path) -> bool:
    retries = Retry(connect=5, read=3, redirect=3)
    http = urllib3.PoolManager(retries=retries)
    try:
        with http.request('GET',url, preload_content=False) as resp, open(filepath, 'wb') as h_out:
            shutil.copyfileobj(resp, h_out)
        return True
    except urllib3.exceptions.HTTPError as e:
        logging.error(e)
        return False


def determine_data_dir(folder:Optional[str]=None, sub_dir:Optional[str]=None) -> pathlib.Path:
    """Utility function to check if a folder is provided or if the default should be used, also creates the dir if not existing"""
    if folder is not None:
        data_dir = pathlib.Path(folder)
    else:
        data_dir = DEFAULT_DATA_DIR
    if sub_dir is not None:
        data_dir = data_dir / sub_dir 
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

def download_dump(year:int, month:int, force:bool=False, folder:Optional[str]=None) -> None:
    data_dir = determine_data_dir(folder, "compressed")
    ext = infer_extension(year, month)
    date_str = f"{year}-{str(month).zfill(2)}"
    if year < 2020:  # monthly archive files, varying extenions
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
    logging.info(f"Downloading {len(urls)} file(s) to {data_dir} for {date_str}")
    for url in urls:
        fn = url.split("/")[-1]
        fp = data_dir / fn
        if force is True or not fp.is_file():
            logging.info(f"Downloading {fn}")
            success = download_file(url, fp)
            if success is True:
                logging.info(f"Downloaded {fn}")
            else:
                logging.warning(f"Did not download {fn}")
        else:
            logging.info(f"File {fn} already exists, not downloading again unless asked via --force")


def batch_download_dumps(from_year:int, to_year:int, force:bool=False, folder:Optional[str]=None) -> None:
    if from_year > to_year:
        from_year, to_year = to_year, from_year
    for y in range(from_year, to_year+1):
        if y > 2005:
            for m in range(1, 13):
                download_dump(year=y, month=m, force=force, folder=folder)
        else:
            if y == 2005:
                download_dump(year=y, month=12, force=force, folder=folder)
            else:
                logging.wwrning(f"No data available for {y}")


def list_files(downloaded:bool=True, extracted:bool=True, folder:Optional[str]=None) -> None:
    if downloaded is True:
        data_dir = determine_data_dir(folder, "compressed")
        print("Downloaded comment dumps:")
        files = sorted(data_dir.glob("RC_*-*.*"))    
        for i, fp in enumerate(files):
            print(i, fp, sep="\t")
    if extracted is True:
        data_dir = determine_data_dir(folder, "extracted")
        print("Extracted comment dumps:")
        files = sorted(data_dir.glob("*/*_*-*"))    
        for i, fp in enumerate(files):
            print(i, fp, sep="\t")




if __name__ == "__main__":
    fire.Fire({
        'download': download_dump,
        'batch-download': batch_download_dumps,
        'extract': extract_from_dump,
        "batch-extract": batch_extract_from_dumps,
        'split': split_extracted,
        "list": list_files,
    })