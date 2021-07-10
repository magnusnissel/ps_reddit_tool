# ps_dump_extractor

## About

This script provides a python CLI tool that allows you to download __Reddit__ comment dumps from __pushshift.io__ and to then extract the comments for a particular subreddit.
The comments are split into uncompressed files (by subreddit & month) using the same basic structure (one JSON object per line containing the data for one comment).
The script can also be used to further split these exctracted subreddit files into daily chunks (using each comment's UTC creation date).

## Example

### Downloading

- Download dumps for March 2006, unless the file already exists:

    ```python3 cli.py download 2016 3```

- Download dumps for February 2020, overwriting existing files:

    ```python3 cli.py download 2020 2 --force=True```


## Configuration

By default the files are downloaded in a folder named __ps_dump_extractor__ within the current user's home directory. This default change be permanently changed by editing the line in _config.py_. Alternatively, the _--folder_ arguments can be passed to any given command to instead use that folder for both reading & writing.

## N.B.

This is script is provided as-is without any warranty and guarantee of functionality.

Given that it is dependent on the availablily of the files provided by  __pushshift.io__ it is will be impacted by any changes that the team behind __pushshift.io__ may make to their directory structures, file formats, naming conventions or the content of the dumps.

This script is not endorsed or affiliated with either __pushshift.io__ or __Reddit__.
