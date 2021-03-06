# ps_reddit_tool

## About

This script provides a python CLI tool that allows you to download __Reddit__ comment dumps from __pushshift.io__ and to then extract the comments for a particular subreddit.
The comments are split into uncompressed files (by subreddit & month) using the same basic structure (one JSON object per line containing the data for one comment) as the original.

The script can also be used to further split these extracted subreddit files into daily chunks (using each comment's UTC creation date) to create smaller files to work with.


## Configuration

By default the files are downloaded in a folder named __ps_reddit_tool__ within the current user's home directory. This default folder be changed by calling 
```python3 cli.py config folder PATH_TO_FOLDER```. This information is stored in the script directory inside the _local_config.json_ file.

## Example

### Downloading

- Download dump for March 2006, unless the file exists already:

    ```python3 cli.py comments download 2016 3```

- Download dumps for February 2020, overwriting existing files:

    ```python3 cli.py comments download 2020 2 --force=True```

- Batch download dumps for all months of the years 2016, 2017, ann 2018

    ```python3 cli.py comments download 2016 2018```

### Extraction

- Exctract all June 2019 comments from the WNBA subreddit unless the exctracted file exists already

    ```python3 cli.py comments extract 2019 6 wnba```

- Extract all comments from the WNBA subreddit from January 2018 to December 2019, overwriting any existing subreddit files 

    ```python3 cli.py comments extract 2018 2019 wnba --force=True```

### Splitting

During extraction one file is created for each subreddit & month. The _split_ command can be used to break these extracted files down into smaller daily files. 

- Create daily subreddit comment files from the previously-extracted WNBA June 2019 file

    ```python3 cli.py comments split 2019 6 wnba```

- Create daily subreddit comment files from the previously-extracted WNBA June 2019 file & then delete the original (monthly) file

    ```python3 cli.py comments split 2019 6 wnba --delete_source=True```

### Listing and checking

- List all compressed files (with size) that were downloaded

    ```python3 cli.py comments list```


- Also list all extracted / split subreddit files

    ```python3 cli.py comments list --extracted=True```

- List all compressed files and display if the checksum matches the one provided by pushshift.io (if available)

    ```python3 cli.py comments list --verify=True```

- List all compressed files, delete all files that have an unexpected checksum

    ```python3 cli.py comments list --verify=True --delete_mismatched=True```

- List all compressed files and delete all files that are empty (0 bytes)

    ```python3 cli.py comments list --delete_empty=True```

- List all compressed files and delete all files that are undersized (by more than 20%) compared to the expected size (as given by the Content-Length header)

    ```python3 cli.py comments list --delete_undersized=True```

- As above, but delete all files that aren't larger than 95% of the expected size

    ```python3 cli.py list --delete_undersized=True --size_ratio=0.95```




## N.B.

This is script is provided as-is without any warranty and guarantee of functionality.

Given that it is dependent on the availablily of the files provided by  __pushshift.io__ it will be impacted by any changes that the team behind __pushshift.io__ may make to their directory structures, file formats, naming conventions or the content of the dumps.

This script and its author(s) are not endorsed or affiliated with either __pushshift.io__ or __Reddit__.
