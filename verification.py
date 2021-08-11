
import hashlib
import pathlib
import downloading
import logging
import helpers
from typing import Optional

def _parse_checksum_file(fp:pathlib.Path) -> dict:
    check = fp.read_text("utf-8")
    check = [ln.strip().split() for ln in check.split("\n") if len(ln.strip()) > 0]
    check = {v.strip(): k.strip() for k,v in check}
    return check


def _read_file_hash(fp:pathlib.Path):
    hasher = hashlib.sha256()
    with open(fp, 'rb') as fh:
        while True:
            data = fh.read(8192)
            if len(data) == 0:
                break
            else:
                hasher.update(data)
    return hasher.hexdigest()


def list_files(downloaded:bool=True, extracted:bool=False, folder:Optional[str]=None, verify:bool=False, delete_mismatched:bool=False,
               delete_empty:bool=False, delete_undersized:bool=False, size_ratio=0.8) -> None:
    if verify is True:
        logging.info("Downloading the most recent checksum file")
        check_fp = downloading.download_checksum_file("comments")
        check_map = _parse_checksum_file(check_fp)

    if downloaded is True:
        data_dir = helpers.determine_data_dir(folder, "compressed")
        logging.info("Downloaded comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob("RC_*-*.*"))):
            check_str = ""
            bad_check = False
            if verify is True:
                try:
                    checksum = check_map[fp.name]
                except KeyError:
                    check_str = f" (No checksum found, unable to verify)"
                else:
                    file_hash = _read_file_hash(fp)
                    if checksum == file_hash:
                        check_str = f" (Checksum verified)"
                    else:
                        bad_check= True
                        check_str = f" (Checksum mismatch: Expected {checksum}, got {file_hash})"
                       

            if bad_check is False:
                logging.info(f"{i} {fp} ({helpers.get_file_size_info_str(fp)}){check_str}")
            else:
                logging.warning(f"{i} {fp} ({helpers.get_file_size_info_str(fp)}){check_str}")
                if delete_mismatched is True:
                    fp.unlink()
                    if not fp.is_file():
                        logging.warning(f"Deleted file with invalid checksum: {fp}")
            if delete_empty is True and fp.stat().st_size == 0:
                try:
                    fp.unlink()
                except FileNotFoundError:
                    pass
                else:
                    logging.warning(f"Deleted 0 byte file: {fp}")
            elif delete_undersized is True:  # Check the url again for the approximate expected file size, if the file is significantly below it, then delete it
                url = f"https://files.pushshift.io/reddit/comments/{fp.name}"
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
                    if file_size < threshold:
                        logging.warning(f"Downloaded file is more than {(1-size_ratio)*100}% smaller than expected. (Expected {helpers.convert_size_to_str(dl_file_size)}, found {helpers.convert_size_to_str(file_size)}")
                        try:
                            fp.unlink()
                        except FileNotFoundError:
                            pass
                        else:
                            logging.warning(f"Deleted undersized file: {fp}")


    if extracted is True:
        data_dir = helpers.determine_data_dir(folder, "extracted")
        print("Extracted comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob("**/*_*-*"))):
            logging.info(f"{i} {fp} ({helpers.get_file_size_info_str(fp)})")

