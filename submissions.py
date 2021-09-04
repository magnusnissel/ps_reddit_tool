from helpers import AbstractTool
from typing import Union
import logging
import downloading
import extraction
import processing
import verification


class SubmissionTool(AbstractTool):
    def download(self, since: Union[str, int], until: Union[str, int, None], force: bool = False) -> None:
        self._initialize_dates(since, until)
        logging.info(f"Downloading available submission dumps from {self._get_date_range_str()}")
        for p in self.periods:
            downloading.download_dump(prefix="RS", year=p[0], month=p[1], force=force)

    def extract(
        self, since: Union[str, int], until: Union[str, int, None], subreddit: str, force: bool = False
    ) -> None:
        self._initialize_dates(since, until)
        subreddit = subreddit.lower().strip()
        logging.info(f"Extracting downloaded comments for subreddit '{subreddit}' from {self._get_date_range_str()}")
        for p in self.periods:
            extraction.extract_from_dump("RS", year=p[0], month=p[1], subreddit=subreddit, force=force)

    def split(self, since: Union[str, int], until: Union[str, int, None], subreddit: str) -> None:
        self._initialize_dates(since, until)
        subreddit = subreddit.lower().strip()
        logging.info(
            f"Splitting monthly '{subreddit}' submission files from {self._get_date_range_str()} into daily files"
        )
        for p in self.periods:
            processing.split_extracted("RS", year=p[0], month=p[1], subreddit=subreddit)

    def checksize(self, size_ratio=0.8) -> None:
        verification.check_filesizes("RS", size_ratio)

    def checkhash(self) -> None:
        verification.check_filehashes("RS")

    def list(self, downloaded: bool = True, extracted: bool = False) -> None:
        verification.list_files("RS", downloaded, extracted)
