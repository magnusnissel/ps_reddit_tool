import json


def infer_extension(year:int, month:int) -> str:
    ext = "zst"
    if (year == 2018 and month < 10) or (year == 2017 and month == 12):
        ext = "xz"
    elif year < 2017 or (year == 2017 and month < 12):
        ext = "bz2"
    return ext


def is_relevant_ln(ln:str, subreddit:str) -> bool:
    d = json.loads(ln.decode("utf-8"))
    return d["subreddit"].lower() == subreddit