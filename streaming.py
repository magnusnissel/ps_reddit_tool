import json
import logging
import pathlib
import datetime
from prawtools import PrawJsonEncoder, authenticate_with_praw
from config import LOCAL_CONFIG_FP, DATA_DIR


class StreamTool:
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
    def _get_output_path(
        prefix: str, subreddit: str, only_id: bool = False
    ) -> pathlib.Path:
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

    def submissions(
        self,
        subreddit: str,
        only_id: bool = False,
        skip_existing: bool = False,
        chunksize: int = 100,
        max_chunk_duration=300,
    ):
        self._check_auth_info()
        subreddit = subreddit.lower().strip()
        self.reddit = authenticate_with_praw(self.credentials)
        logging.info(f"Streaming submissions in subreddit '{subreddit}'")
        data = []
        i = 0
        last_saved = (
            datetime.datetime.utcnow()
        )  # to allow early saving even if chunksize isn't met

        for submission in self.reddit.subreddit(subreddit).stream.submissions(
            skip_existing=skip_existing
        ):
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

    def comments(
        self,
        subreddit: str,
        only_id: bool = False,
        skip_existing: bool = False,
        chunksize: int = 100,
        max_chunk_duration=300,
    ):
        self._check_auth_info()
        subreddit = subreddit.lower().strip()
        self.reddit = authenticate_with_praw(self.credentials)
        logging.info(f"Streaming comments in subreddit '{subreddit}'")
        data = []
        i = 0
        last_saved = (
            datetime.datetime.utcnow()
        )  # to allow early saving even if chunksize isn't met
        for comment in self.reddit.subreddit(subreddit).stream.comments(
            skip_existing=skip_existing
        ):
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
