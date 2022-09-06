# Daily File Generation

This repo will eventually be for the containerized version of daily file code.

The important code is in `processing.py` which will contain processing functions for each
of our daily file data sources (rads, S6, GSFC). Independent from this repo will be a data
harvesting repo - this daily file generation code assumes pass files from each source
are staged on one of our buckets.


## Rads processing notes

1. rads_dates.py:
Extract from file: TIME, LATS, LONS, SSH, SEA_ICE, FLAG1, FLAG2, Satellite, Phase, TRACK_ID, ColTitle, latencies, bad_tracks
Modifications: TIME+= equator crossing time, SEA_ICE = seaice_conc or zeros np array, TRACK_ID = np array of track_id

2. rads_dates2.py:
Works on sats2 = ['TOPEX','JASON-1','JASON-2','JASON-3'] first, then sats1 = ['ERS-1','ERS-2','ENVISAT1','CRYOSAT2','SARAL','SNTNL-3A','SNTNL-3B']:
    getAllPass() -> grabs all .jsons that were saved in rads_dates.py
    data_sort_dump() -> loops through all time steps:
        - removes large absolute values
        - applies ssh bias
        - creates smoothed ssh
        - subsets data that falls on day
        - builds up complete "thisday" and "nextday" dictionaries - why both today and tomorrow?
        - makes h5 file 'alt_ssh{0:d}{1:02d}{2:02d}.h5'

What we want changed:
- Remove all intermediary save to disks
- End result is netcdf
- Define some attributes (current daily files have none)
- sat_id as attr, not array