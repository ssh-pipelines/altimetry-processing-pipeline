# Daily File Generation

*NOTE: repo is in active development and some or all functionalities described in the README may not be working or implemented.*

Containerized software that supports generation of "Daily Files" used in the SSHA data pipeline. Container generates a single Daily File for a single satellite source. 

Currently supported sources are:
- GSFC altimeter data (`MERGED_TP_J1_OSTM_OST_CYCLES_V51`)
- Sentinel 6 altimeter data support in progress

## Overview

For data sources available from PODAAC and are thus available in CMR, CMR is queried for a given date which will return none, or 1, or more granules containing data on the given date. Each granule is harmonized and merged (if necessary) to create a single netCDF.

Containers are given date and source parameters and execute the following steps:
1. Query CMR for granules for date and source
2. Process each granule. This includes data subsetting, smoothing, creation of data flags, and general harmonization to create a consistent Daily File product.
3. Processed granules are merged if needed, and saved as a netCDF before being uploaded to an S3 bucket.

## Building the image
From the root directory after cloning the repo:
```
docker build -t daily_files:latest .
```

## Running the container

```
docker run -e DATE={date} -e SOURCE={input_source} daily_files:latest
```
where `date` is of the format %Y%m%d and `input_source` is one of the support data sources: [`GSFC`, `S6`]