import praw
import logging
import json


def authenticate_with_praw(credentials: dict) -> praw.Reddit:

    reddit = praw.Reddit(
        client_id=credentials["clientId"],
        client_secret=credentials["clientSecret"],
        password=credentials["password"],
        user_agent=credentials["userAgent"],
        username=credentials["userName"],
    )

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
            # print(sorted(d.keys()))
            d.pop("_reddit", None)
            return d
        else:
            return super(PrawJsonEncoder, self).default(obj)
