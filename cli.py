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
import praw
from config import LOCAL_CONFIG_FP, DATA_DIR

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

    def auth(self, id:str, secret:str, password:str, agent:str, name:str) -> None:
        d = json.loads(LOCAL_CONFIG_FP.read_text())
        d["auth"] = {
            "clientId": id,
            "clientSecret": secret,
            "password": password,
            "userAgent": agent,
            "userName": name
        }
        logging.info(f"Reddit bot auth data saved")
        LOCAL_CONFIG_FP.write_text(json.dumps(d, indent=4))




def authenticate_with_praw(credentials: dict) -> praw.Reddit:

    reddit = praw.Reddit(client_id=credentials["clientId"], client_secret=credentials["clientSecret"],
                         password=credentials["password"], user_agent=credentials["userAgent"], username=credentials["userName"])

    logging.info(f"Authenticated as {reddit.user.me()}")
    return reddit

class PrawJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, praw.models.reddit.subreddit.Subreddit):
            d = obj.__dict__
            d.pop("_reddit", None)
            d.pop("_fetched", None)
            return d
        elif isinstance(obj, praw.models.Redditor):
            d = obj.__dict__
            d.pop("_reddit", None)
            d.pop("_listing_use_sort", None)
            d.pop("_fetched", None)
            return d
        elif isinstance(obj, praw.models.reddit.poll.PollData):
            d = obj.__dict__
            d.pop("_reddit", None)
            d.pop("_user_selection", None)
            return d
        elif isinstance(obj, praw.models.reddit.poll.PollOption):
            d = obj.__dict__
            #print(sorted(d.keys()))
            d.pop("_reddit", None)
            return d
        else:
            return super(PrawJsonEncoder, self).default(obj)


class StreamTool():
    
    def __init__(self) -> None:
        self.reddit = None


    def _check_auth_info(self) -> None:
        d = json.loads(LOCAL_CONFIG_FP.read_text())
        problem = False
        try:
            if len(d["auth"].keys()) == 0:
                exit("Missing reddit bot auth information")
            for k, v in d["auth"].items():
                if len(v.strip()) < 3:
                    exit("Invalid reddit bot auth information")
        except KeyError:
            exit("Missing reddit bot auth information")
        else:
            self.credentials = d["auth"]

    @staticmethod        
    def _prep_json_str(d: dict) -> str:
        d.pop("_reddit", None)
        d.pop("_fetched", None)
        d.pop("_comments_by_id", None)
        return json.dumps(d, indent=None, cls=PrawJsonEncoder)

    @staticmethod
    def _get_output_path(prefix:str, subreddit:str, only_id:bool=False) -> pathlib.Path:
        now = datetime.datetime.utcnow()
        dstr = now.strftime("%Y%m%d")
        dn = DATA_DIR / "streamed" / subreddit / dstr
        dn.mkdir(parents=True, exist_ok=True)
        ts = int(now.timestamp())
        if only_id is True:
            fp = dn / f"{prefix}_ids_{subreddit}_{ts}"
        else:
            fp = dn / f"{prefix}_{subreddit}_{ts}"
        return fp

    def submissions(self, subreddit:str, only_id:bool=False, skip_existing:bool=False, chunksize:int=100, max_chunk_duration=300):
        self._check_auth_info()
        subreddit = subreddit.lower().strip()
        self.reddit = authenticate_with_praw(self.credentials)
        logging.info(f"Streaming submissions in subreddit '{subreddit}'")
        data = []
        i = 0
        last_saved = datetime.datetime.utcnow()  # to allow early saving even if chunksize isn't met

        for submission in self.reddit.subreddit(subreddit).stream.submissions(skip_existing=skip_existing):
            if only_id is True:
                data.append(submission.id)
            else:
                data.append(self._prep_json_str(submission.__dict__))
            i += 1
            duration = (datetime.datetime.utcnow() - last_saved).total_seconds()
            if duration > max_chunk_duration or i >= chunksize:
                if len(data) > 0:
                    fp = self._get_output_path("rs", subreddit, only_id)
                    fp.write_text("\n".join(data))
                    data = []
                    logging.info(fp.name)
                    i = 0

    def comments(self, subreddit:str, only_id:bool=False, skip_existing:bool=False, chunksize:int=100, max_chunk_duration=300):
        self._check_auth_info()
        subreddit = subreddit.lower().strip()
        self.reddit = authenticate_with_praw(self.credentials)
        logging.info(f"Streaming comments in subreddit '{subreddit}'")
        data = []
        i = 0
        last_saved = datetime.datetime.utcnow()  # to allow early saving even if chunksize isn't met
        for comment in self.reddit.subreddit(subreddit).stream.comments(skip_existing=skip_existing):
            if only_id is True:
                data.append(comment.id)
            else:
                data.append(self._prep_json_str(comment.__dict__))
            i += 1
            duration = (datetime.datetime.utcnow() - last_saved).total_seconds()
            if duration > max_chunk_duration or i >= chunksize:
                if len(data) > 0:
                    fp = self._get_output_path("rc", subreddit, only_id)
                    fp.write_text("\n".join(data))
                    data = []
                    logging.info(fp.name)
                    i = 0
                    last_saved = datetime.datetime.utcnow()


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
        self.stream = StreamTool()

if __name__ == "__main__":
    fire.Fire(CommandLineInterface)

