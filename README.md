# migrationtools
Tools to assist in data migration

## Chunking tool

Chunking script/utilities

```
$> bin/chunk.py -h
usage: chunk.py [-h] -i INPUTFILE [-s CHUNKSIZE] [-b BLOCK] [-v]

Utility to split up large files into smaller files for parallel transfers

options:
  -h, --help            show this help message and exit
  -i INPUTFILE, --inputfile INPUTFILE
                        The file to break apart (required)
  -s CHUNKSIZE, --chunksize CHUNKSIZE
                        Maximum size of each chunk, in MB. Defaults to 100MB. WARNING: This should not be set to more than 80 percent of the
                        system's available memory
  -b BLOCK, --block BLOCK
                        Generate/recreate a specific block. Requires a start and end block. Using this option will not output a manifest file and
                        the -s/--chunksize flag will be ignored. Enter the BLOCK value as a hyphen-separated string (eg. 12345-678901)
  -v, --verbose         Add verbosity

This software is copyright 2023, ServiceNow
```
