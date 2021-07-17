import logging
from typing import Optional

import fire

from helpers import determine_data_dir
import downloading
import extraction
import processing


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler("ps_dump_extractor.log"),
                              logging.StreamHandler()])


def list_files(downloaded:bool=True, extracted:bool=True, folder:Optional[str]=None) -> None:
    data_dir = determine_data_dir(folder)
    if downloaded is True:
        print("Downloaded comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob("RC_*-*.*"))):
            print(i, fp, sep="\t")
    if extracted is True:
        print("Extracted comment dumps:")
        for i, fp in enumerate(sorted(data_dir.glob("*/*_*-*"))):
            print(i, fp, sep="\t")



if __name__ == "__main__":
    fire.Fire({
        'download': downloading.download_dump,
        'batch-download': downloading.batch_download_dumps,
        'extract': extraction.extract_from_dump,
        "batch-extract": extraction.batch_extract_from_dumps,
        'split': processing.split_extracted,
        "list": list_files,
    })