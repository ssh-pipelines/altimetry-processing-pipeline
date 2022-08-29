# Daily File Generation

This repo will eventually be for the containerized version of daily file code.

The important code is in `processing.py` which will contain processing functions for each
of our daily file data sources (rads, S6, GSFC). Independent from this repo will be a data
harvesting repo - this daily file generation code assumes pass files from each source
are staged on one of our buckets.
