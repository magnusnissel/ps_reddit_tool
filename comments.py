from helpers import AbstractTool
from typing import Union, Tuple, Optional
import logging
import downloading
import extraction
import processing
import verification


class CommentTool(AbstractTool):
    def __init__(self) -> None:
        super().__init__()

    def download(
        self,
        since: Union[str, int],
        until: Union[str, int, None],
        force: bool = False,
        checkhash: bool = False,
        checksize: bool = False,
        retry: bool = False,
        max_attempts: int = 3,
    ) -> None:
        self._initialize_dates(since, until)
        logging.info(f"Downloading available comment dumps from {self._get_date_range_str()}")
        for p in self.periods:
            downloading.download_dump(
                prefix="RC",
                year=p[0],
                month=p[1],
                force=force,
                checkhash=checkhash,
                checksize=checksize,
                retry=retry,
                max_attempts=max_attempts,
            )

    def extract(
        self, since: Union[str, int], until: Union[str, int, None], subreddit: str, force: bool = False
    ) -> None:
        self._initialize_dates(since, until)
        subreddit = subreddit.lower().strip()
        logging.info(f"Extracting downloaded comments for subreddit '{subreddit}' from {self._get_date_range_str()}")
        for p in self.periods:
            extraction.extract_from_dump("RC", year=p[0], month=p[1], subreddit=subreddit, force=force)

    def split(self, since: Union[str, int], until: Union[str, int, None], subreddit: str) -> None:
        self._initialize_dates(since, until)
        subreddit = subreddit.lower().strip()
        logging.info(
            f"Splitting monthly '{subreddit}' comment files from {self._get_date_range_str()} into daily files"
        )
        for p in self.periods:
            processing.split_extracted("RC", year=p[0], month=p[1], subreddit=subreddit)

    def checksize(self, size_ratio=0.8) -> None:
        verification.check_filesizes("RC", size_ratio)

    def checkhash(self) -> None:
        verification.check_filehashes("RC")

    def list(self, downloaded: bool = True, extracted: bool = False) -> None:
        verification.list_files("RC", downloaded, extracted)
