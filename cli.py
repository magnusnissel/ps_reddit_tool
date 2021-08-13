import logging
import fire
from typing import  Optional, Tuple, Union
import datetime
import downloading
import extraction
import processing
import verification
import json
import pathlib
from config import LOCAL_CONFIG_FP

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler("ps_dump_extractor.log"),
                              logging.StreamHandler()])


class AbstractTool():

    def __init__(self):
        self.since, self.until = {}, {}
        self.periods = []


    @staticmethod    
    def _interpret_date_input(date_str: str) -> dict:
        d = {}
        try:
            dt = datetime.datetime.strptime(date_str, "%Y-%m")
        except ValueError:
            try:
                dt = datetime.datetime.strptime(date_str, "%Y")
            except ValueError as e:
                raise(e)
            else:
                d = {"year": dt.year, "month": None}
        else:
            d = {"year": dt.year, "month": dt.month}
        return d

    def _set_periods(self) -> list:
        years = [y for y in range(self.since["year"], self.until["year"]+1)]
        self.periods = []
        for y in years:
            if y not in (self.since["year"], self.until["year"]):  # for any years in between always get months 1-12
                months = [m for m in range(1, 13)]
            elif self.since["year"] == self.until["year"]:
                months = [m for m in range(self.since["month"], self.until["month"]+1)]
            elif y == self.since["year"]:
                months = [m for m in range(self.since["month"], 13)]
            elif y == self.until["year"]:
                months = [m for m in range(1, self.until["month"]+1)]
            self.periods += [(y, m) for m in months]

    def _interpret_date_range(self, since:Union[str, int], until:Union[str, int, None]) -> Tuple[dict]:
        since = self._interpret_date_input(str(since))  #  if only a year as given, Fire interprets is as int
        if until is None:
            until = since
        else:
            until = self._interpret_date_input(str(until))
        #If no month specified, assume from January of the first year until December of the last year
        if since["month"] is None:
            since["month"] = 1
        if until["month"] is None:
            until["month"] = 12
        self.since = since
        self.until = until


    def _initialize_dates(self, since:Union[str, int], until:Union[str, int, None]):
        self._interpret_date_range(since, until)
        self._set_periods()

    def _get_date_range_str(self, sep:str="to"):
        return f"{self.periods[0][0]}-{str(self.periods[0][1]).zfill(2)} {sep} {self.periods[-1][0]}-{str(self.periods[-1][1]).zfill(2)}"


class SubmissionTool(AbstractTool):

    def download(self, since:Union[str, int], until:Union[str, int, None], force:bool=False) -> None:
        self._initialize_dates(since, until)
        logging.info(f"Downloading available submission dumps from {self._get_date_range_str()}")
        for p in self.periods:
            downloading.download_dump(year=p[0], month=p[1], comments=False, submissions=True, force=force)
    

    def extract(self, since:Union[str, int], until:Union[str, int, None], subreddit:str, force:bool=False) -> None:
        self.placeholder()

    def split(self, since:Union[str, int], until:Union[str, int, None], subreddit:str, delete_source:bool=False, force:bool=False) -> None:
        self.placeholder()

    def list(self, downloaded:bool=True, extracted:bool=False, verify:bool=False, delete_mismatched:bool=False,
             delete_empty:bool=False, delete_undersized:bool=False, size_ratio=0.8) -> None:
        self.placeholder()

    def placeholder(self):
        exit("Sorry, this functionality is not yet implemented!")



class ConfigTool():

    def folder(self, folder:str) -> None:
        dn = pathlib.Path(folder)
        if not dn.is_dir():
            try:
                dn.mkdir(parents=True, exist_ok=True)
            except IOError:
                exit(f"It looks like {folder} is not a valid folder path")
        d = {"dataFolder": folder}
        logging.info(f"Data folder set: '{dn}'")
        LOCAL_CONFIG_FP.write_text(json.dumps(d, indent=4))



class CommentTool(AbstractTool):

    def __init__(self) -> None:
        super().__init__()

    def download(self, since:Union[str, int], until:Union[str, int, None], force:bool=False) -> None:
        self._initialize_dates(since, until)
        logging.info(f"Downloading available comment dumps from {self._get_date_range_str()}")
        for p in self.periods:
            downloading.download_dump(year=p[0], month=p[1], comments=True, submissions=False, force=force)

    def extract(self, since:Union[str, int], until:Union[str, int, None], subreddit:str, force:bool=False) -> None:
        self._initialize_dates(since, until)
        subreddit = subreddit.lower().strip()
        logging.info(f"Extracting downloaded comments for subreddit '{subreddit}' from {self._get_date_range_str()}")
        for p in self.periods:
            extraction.extract_from_dump(year=p[0], month=p[1], subreddit=subreddit, force=force)

    def split(self, since:Union[str, int], until:Union[str, int, None], subreddit:str, delete_source:bool=False, force:bool=False) -> None:
        self._initialize_dates(since, until)
        subreddit = subreddit.lower().strip()
        logging.info(f"Splitting monthly '{subreddit}' files from {self._get_date_range_str()} into daily files")
        for p in self.periods:
            processing.split_extracted(year=p[0], month=p[1], subreddit=subreddit, delete_source=delete_source, force=force)

    def list(self, downloaded:bool=True, extracted:bool=False, verify:bool=False, delete_mismatched:bool=False,
             delete_empty:bool=False, delete_undersized:bool=False, size_ratio=0.8) -> None:
        #TODO: Refactor list / checksum functionality from the ground up
        verification.list_files(downloaded, extracted, verify, delete_mismatched, delete_empty, delete_undersized, size_ratio)


class CommandLineInterface():

    def __init__(self):
        self.comments = CommentTool()
        self.submissions = SubmissionTool()
        self.config = ConfigTool()

if __name__ == "__main__":
    fire.Fire(CommandLineInterface)

