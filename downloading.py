import logging
import pathlib
import datetime
import shutil
from typing import Optional
import urllib3
import multiprocessing as mp
import time
from helpers import infer_extension, get_file_size_info_str, convert_size_to_str
from config import DATA_DIR


def download_checksum_file(kind: str = "comments") -> pathlib.Path:
    if kind == "comments":
        url = "https://files.pushshift.io/reddit/comments/sha256sum.txt"
    elif kind == "submissions":
        url = "https://files.pushshift.io/reddit/submissions/sha256sums.txt"
    else:
        raise ValueError("Invalid value for 'kind' in download_checksum_file")
    fp = DATA_DIR / f"sha256sums_{kind}.txt"
    _download_file(
        url, fp, monitor=False
    )  # TODO: Raise appropriate exception if dowload fails
    return fp


def _monitor_filepath(fp: pathlib.Path, target_size: Optional[float] = None) -> None:
    time.sleep(30)  # initial delay
    while True:
        try:
            if target_size is not None:
                logging.info(
                    f"{get_file_size_info_str(fp)} / {convert_size_to_str(target_size)} downloaded so far ({fp.name})"
                )
            else:
                logging.info(
                    f"{get_file_size_info_str(fp)} downloaded so far ({fp.name})"
                )
        except FileNotFoundError:
            time.sleep(10)
        else:
            time.sleep(60)


def _check_url_content_length(url: str) -> int:
    http = urllib3.PoolManager()
    resp = http.request("GET", url, preload_content=False)
    if resp.status == 200:
        return int(resp.headers.get("Content-Length"))
    else:
        return -1


def _download_file(
    url: str,
    fp: pathlib.Path,
    monitor: bool = True,
    target_size: Optional[float] = None,
) -> bool:
    retries = urllib3.util.retry.Retry(connect=5, read=3, redirect=3)
    http = urllib3.PoolManager(retries=retries)
    if monitor is True:
        p_mon = mp.Process(target=_monitor_filepath, args=(fp, target_size))
        p_mon.start()
    try:
        with http.request("GET", url, preload_content=False) as resp, open(
            fp, "wb"
        ) as h_out:
            shutil.copyfileobj(resp, h_out)
        if monitor is True:
            p_mon.terminate()
            p_mon.join()
        return True
    except urllib3.exceptions.HTTPError as e:
        if monitor is True:
            p_mon.terminate()
            p_mon.join()
        logging.error(e)
        return False


def _get_paths_for_urls(urls: list, data_dir: pathlib.Path) -> "list[pathlib.Path]":
    files = [data_dir / u.split("/")[-1] for u in urls]
    return files


def download_dump(
    year: int,
    month: int,
    comments: bool = True,
    submissions: bool = True,
    force: bool = False,
) -> None:
    data_dir = DATA_DIR / "compressed"
    ext = infer_extension(year, month)
    date_str = f"{year}-{str(month).zfill(2)}"
    urls = []
    if comments is True:
        urls.append(
            f"https://files.pushshift.io/reddit/comments/RC_{date_str}.{ext}"
        )  # varying extenions

    if submissions is True:
        urls.append(f"https://files.pushshift.io/reddit/submissions/RS_{date_str}.zst")

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
        logging.info(f"Downloading {len(dl_paths)} file to {data_dir} for {date_str}")
    if force is False and len(skip_paths) > 0:
        logging.info(
            f"Skipping {len(skip_paths)} existing files for {date_str} (--force=True to override this)"
        )
    for fp, url in zip(dl_paths, dl_urls):
        if (
            force is True or not fp.is_file()
        ):  # Second check just in case is_file() status has changed

            dl_file_size = _check_url_content_length(url)
            if dl_file_size != -1:
                logging.info(f"Downloading {fp.name} ...")
                logging.info(
                    f"Approximate file size: {convert_size_to_str(dl_file_size)}"
                )
                dl_start = datetime.datetime.utcnow()
                success = _download_file(url, fp, True, dl_file_size)
                duration = (
                    str(datetime.datetime.utcnow() - dl_start).split(".")[0].zfill(8)
                )
                if success is True:
                    logging.info(
                        f"Downloaded {fp.name} in {duration} ({get_file_size_info_str(fp)})"
                    )
                else:
                    logging.warning(
                        f"Failed to download {fp.name} after trying for {duration}"
                    )
