import logging
import pathlib
import datetime
import shutil
from typing import Optional
import urllib3

from helpers import determine_data_dir, infer_extension

def _download_file(url:str, filepath:pathlib.Path) -> bool:
    retries = urllib3.util.retry.Retry(connect=5, read=3, redirect=3)
    http = urllib3.PoolManager(retries=retries)
    try:
        with http.request('GET',url, preload_content=False) as resp, open(filepath, 'wb') as h_out:
            shutil.copyfileobj(resp, h_out)
        return True
    except urllib3.exceptions.HTTPError as e:
        logging.error(e)
        return False

def _get_paths_for_urls(urls:list, data_dir:pathlib.Path) -> "list[pathlib.Path]":
    files = [data_dir / u.split("/")[-1] for u in urls]
    return files

def download_dump(year:int, month:int, force:bool=False, folder:Optional[str]=None) -> None:
    data_dir = determine_data_dir(folder)
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
    paths = _get_paths_for_urls(urls, data_dir)
    dl_urls = []
    dl_paths, skip_paths = [], []
    for fp, url in zip(paths, urls):
        if force is True or not fp.is_file():
            dl_urls.append(url)
            dl_paths.append(fp)
        else:
            skip_paths.append(fp)
    if len(dl_paths) > 0:
        logging.info(f"Downloading {len(dl_paths)} file(s) to {data_dir} for {date_str}")
    if force is False and len(skip_paths) > 0:
        logging.info(f"Skipping {len(skip_paths)} existing files(s) for {date_str} (--force=True to override this)")
    for fp, url in zip(dl_paths, dl_urls):
        if force is True or not fp.is_file():  # Second check just in case is_file() status has changed 
            
            logging.info(f"Downloading {fp.name} ...")
            dl_start = datetime.datetime.utcnow()
            success = _download_file(url, fp)
            duration = str(datetime.datetime.utcnow() - dl_start).split(".")[0].zfill(8)
            if success is True:
                logging.info(f"Downloaded {fp.name} in {duration}")
            else:
                logging.warning(f"Failed to download {fp.name} after trying for  {duration}")


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
                logging.warning(f"No data available for {y}")


