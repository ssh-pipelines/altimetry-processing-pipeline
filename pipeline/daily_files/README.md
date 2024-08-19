# Daily File Generation

Containerized software that supports generation of "Daily Files" used in the NASA SSHA data pipeline. Container generates a single Daily File for a single satellite source. 

Currently supported sources are:
- GSFC altimeter data (`MERGED_TP_J1_OSTM_OST_CYCLES_V51`)
- Sentinel 6 altimeter data (`JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08`, `JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08_UNVALIDATED`, `JASON_CS_S6A_L2_ALT_LR_RED_OST_STC_F`)

## Overview

For data sources available from PODAAC and are thus available in CMR, CMR is queried for a given date which will return none, or 1, or more granules containing data on the given date. Each granule is harmonized and merged (if necessary) to create a single netCDF.

Containers are given date and source parameters and execute the following steps:
1. Query CMR for granules for date and source
2. Process each granule. This includes data subsetting, smoothing, creation of data flags, and general harmonization to create a consistent Daily File product.
3. Processed granules are saved as a netCDF before being uploaded to an S3 bucket.


## Running the container

Container is designed to be run via AWS Lambda. It expects the following parameters via Lambda's `event`:
- `date` (of the form %Y-%m-%d)
- `source` (currently one of `GSFC` or `S6`)
- `satellite` (currently not used although will be when CMEMS support is added)

## Unit tests
A small sample of unitests can be executed in the `tests` directory. The `test_gsfc_processing` and `test_s6_processing` tests will generate a sample netcdf using the provided sample granules found in `tests/testing_granules`.
