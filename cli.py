import logging
import pathlib
import fire
from typing import  Optional
import hashlib

from helpers import determine_data_dir, get_file_size_info_str
import downloading
import extraction
import processing


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler("ps_dump_extractor.log"),
                              logging.StreamHandler()])

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



def list_files(downloaded:bool=True, extracted:bool=False, folder:Optional[str]=None, verify:bool=False, delete_mismatched:bool=False, delete_empty:bool=False) -> None:
    if verify is True:
        logging.info("Downloading the most recent checksum file")
        check_fp = downloading.download_checksum_file("comments")
        check_map = _parse_checksum_file(check_fp)

    if downloaded is True:
        data_dir = determine_data_dir(folder, "compressed")
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
                logging.info(f"{i} {fp} ({get_file_size_info_str(fp)}){check_str}")
            else:
                logging.warning(f"{i} {fp} ({get_file_size_info_str(fp)}){check_str}")
                if delete_mismatched is True:
                    fp.unlink()
                    if not fp.is_file():
                        logging.warning(f"Deleted file with invalid checksum: {fp}")
            if delete_empty is True and fp.stat().st_size == 0:
                logging.warning(f"Deleted 0 byte file: {fp}")


    if extracted is True:
        data_dir = determine_data_dir(folder, "extracted")
        print("Extracted comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob("**/*_*-*"))):
            logging.info(f"{i} {fp} ({get_file_size_info_str(fp)})")



if __name__ == "__main__":
    fire.Fire({
        'download': downloading.download_dump,
        'batch-download': downloading.batch_download_dumps,
        'extract': extraction.extract_from_dump,
        "batch-extract": extraction.batch_extract_from_dumps,
        'split': processing.split_extracted,
        "list": list_files,
    })