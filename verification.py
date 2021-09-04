import hashlib
import pathlib
import downloading
import logging
import helpers
from typing import Optional
from config import DATA_DIR


def _parse_checksum_file(fp: pathlib.Path) -> dict:
    check = fp.read_text("utf-8")
    check = [ln.strip().split() for ln in check.split("\n") if len(ln.strip()) > 0]
    check = {v.strip(): k.strip() for k, v in check}
    return check


def _read_file_hash(fp: pathlib.Path):
    hasher = hashlib.sha256()
    with open(fp, "rb") as fh:
        while True:
            data = fh.read(8192)
            if len(data) == 0:
                break
            else:
                hasher.update(data)
    return hasher.hexdigest()


def _check_filesize(fp: pathlib.Path, size_ratio: float = 0.8):
    if fp.stat().st_size == 0:
        try:
            fp.unlink()
        except FileNotFoundError:
            pass
        else:
            logging.warning(f"Deleted 0 byte file: {fp}")
    else:
        # Check the url again for the approximate expected file size, if the file is significantly below it, then delete it
        if fp.name.startswith("RC_"):
            url = f"https://files.pushshift.io/reddit/comments/{fp.name}"
        elif fp.name.startswith("RS_"):
            url = f"https://files.pushshift.io/reddit/submissions/{fp.name}"
        else:
            raise ValueError("File {fp.name} has an invalid prefix")
        try:
            dl_file_size = downloading._check_url_content_length(url)
        except TypeError:  # e.g. when url does not exist and function returns None
            pass
        else:
            file_size = fp.stat().st_size
            if size_ratio > 0.99:
                size_ratio = 0.99
            elif size_ratio < 0.1:
                size_ratio = 0.1
            threshold = size_ratio * dl_file_size
            pct = round((1 - size_ratio) * 100, 0)
            if file_size < threshold:
                logging.warning(
                    f"Downloaded file is more than {pct}% smaller than expected. (Expected {helpers.convert_size_to_str(dl_file_size)}, found {helpers.convert_size_to_str(file_size)}"
                )
                try:
                    fp.unlink()
                except FileNotFoundError:
                    pass
                else:
                    logging.warning(f"Deleted undersized file: {fp}")


def check_filesizes(prefix: str, size_ratio: float = 0.8) -> None:
    data_dir = DATA_DIR / "compressed"
    for i, fp in enumerate(sorted(data_dir.glob(f"{prefix}_*-*.*"))):
        logging.info(f"{i} {fp} ({helpers.get_file_size_info_str(fp)})")
        _check_filesize(fp, size_ratio)


def check_filehashes(prefix: str) -> None:
    data_dir = DATA_DIR / "compressed"
    logging.info("Downloading the most recent checksum file")
    if prefix == "RC":
        check_fp = downloading.download_checksum_file("comments")
    elif prefix == "RS":
        check_fp = downloading.download_checksum_file("submissions")
    else:
        raise ValueError("Invalid value for 'prefix'")

    check_map = _parse_checksum_file(check_fp)

    for i, fp in enumerate(sorted(data_dir.glob(f"{prefix}_*-*.*"))):
        check_str = ""
        bad_check = False
        try:
            checksum = check_map[fp.name]
        except KeyError:
            check_str = f" (No checksum info found, unable to verify)"
        else:
            file_hash = _read_file_hash(fp)
            if checksum == file_hash:
                check_str = f" – Checksum verified"
            else:
                bad_check = True
                check_str = (
                    f" – Checksum mismatch: Expected {checksum}, got {file_hash}"
                )

        if bad_check is False:
            logging.info(f"{i} {fp} ({helpers.get_file_size_info_str(fp)}){check_str}")
        else:
            logging.warning(
                f"{i} {fp} ({helpers.get_file_size_info_str(fp)}){check_str}"
            )
            try:
                fp.unlink()
            except FileNotFoundError:
                pass
            else:
                logging.warning(f"Deleted file with invalid checksum: {fp}")


def list_files(prefix: str, downloaded: bool = True, extracted: bool = False) -> None:
    if downloaded is True:
        data_dir = DATA_DIR / "compressed"
        logging.info("Downloaded comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob(f"{prefix}_*-*.*"))):
            logging.info(f"{i} {fp} ({helpers.get_file_size_info_str(fp)})")
    if extracted is True:
        data_dir = DATA_DIR / "extracted"
        logging.info("Extracted comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob("**/*_*-*"))):
            logging.info(f"{i} {fp} ({helpers.get_file_size_info_str(fp)})")
