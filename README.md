# ps_dump_extractor

## About

This script provides a python CLI tool that allows you to download __Reddit__ comment dumps from __pushshift.io__ and to then extract the comments for a particular subreddit.
The comments are split into uncompressed files (by subreddit & month) using the same basic structure (one JSON object per line containing the data for one comment).
The script can also be used to further split these exctracted subreddit files into daily chunks (by each comments UTC creation date).

## Example

### Downloading

- Download dumps for March 2006:

    ```python3 cli.py download 2016 3```

- Download dumps for February 2020, overwriting existing files:

    ```python3 cli.py download 2020 2 --force=True```
