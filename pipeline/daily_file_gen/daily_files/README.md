# Daily Files

Generates level-1 (P1) along-track daily files from satellite altimeter data. For each processing date and source, queries NASA's CMR for granules, ingests and harmonizes the raw data, applies quality flagging, MSS correction, smoothing, and basin mapping, then uploads the resulting NetCDF to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

For each processing date, the Lambda:

1. **Validates the source** against `daily_files/config/sources.yaml`.
2. **Enumerates granules** depending on the source's `discovery_type`:
   - **CMR** (`cmr`): Queries NASA CMR for the source's configured collection(s). For multi-collection sources (S6), selects the highest-priority granule per cycle/pass combination.
   - **S3 bucket** (`s3_bucket`): Lists an internal S3 bucket and matches filenames by date. If the source provides a `cycle_index_key`, reads a JSON index mapping cycle filenames to date ranges and returns files whose range overlaps the target date.
3. **Downloads** granule files from PODAAC's S3 bucket (CMR sources) or the source S3 bucket directly (S3 bucket sources).
4. **Ingests** raw files into a normalized `IngestedData` structure (source-specific: extracts SSHA, lat/lon, time, cycle, pass, DAC, and any source-specific fields).
5. **Processes** the ingested data into a daily file dataset:
   - Maps observations to geographic basins using basin shapefiles
   - Creates `nasa_flag` from source-specific quality flags and a rolling median filter
   - Subsets data to the target date and drops duplicate times
   - Applies MSS swap (source MSS to DTU21) using precomputed difference grids
   - Flags land/lake basins in `nasa_flag`
   - Computes `ssha_smoothed` using a 19-point Gaussian-like along-track filter
   - Sets variable and global CF-compliant metadata
6. **Validates** the output dataset against a schema (required global attributes, variables, and per-variable attributes).
7. **Uploads** the P1 daily file to S3 and removes the local temp copy.

If no granules are found for a date, an empty template NetCDF with appropriate metadata is uploaded instead.

## Directory structure

```
daily_files/
├── app.py                                  # Lambda handler (entry point)
├── daily_files/
│   ├── daily_file_job.py                   # Job orchestration, save/upload, empty template
│   ├── config/
│   │   ├── source_config.py                # Dataclasses + YAML loader (lazy-cached)
│   │   ├── sources.yaml                    # Per-source config (collections, MSS, smoothing)
│   │   ├── dataset_schema.py               # Output schema definition + validation
│   │   └── paths.py                        # Reference file directory paths
│   ├── fetching/
│   │   ├── enumerator.py                   # Abstract Enumerator + FileRef dataclass
│   │   ├── cmr_enumerator.py               # GSFCEnumerator, S6Enumerator (CMR queries)
│   │   ├── s3_bucket_enumerator.py         # S3BucketEnumerator (S3 listing + cycle index)
│   │   └── downloader.py                   # S3Downloader (PODAAC credentials)
│   ├── ingestion/
│   │   ├── ingest.py                       # Abstract Ingestor + IngestedData dataclass
│   │   ├── gsfc_ingest.py                  # GSFCIngestor (pass LUT, DAC from NOIB cycles)
│   │   └── s6_ingest.py                    # S6Ingestor (grouped NetCDF extraction)
│   ├── processing/
│   │   ├── daily_file.py                   # Abstract DailyFile base class
│   │   ├── gsfc_daily_file.py              # GSFCDailyFile (GSFC flag splitting, manual outliers)
│   │   ├── s6_daily_file.py                # S6DailyFile (S6 flag logic, MSS sol1/sol2 correction)
│   │   └── smoothing.py                    # 19-point Gaussian-like SSHA smoothing filter
│   └── ref_files/
│       ├── empty_templates/                # Empty NetCDF templates per source
│       ├── mss_diffs/                      # MSS difference grids (DTU15/18 minus DTU21)
│       ├── basin/                          # Basin/lake polygon shapefiles
│       └── complete_gsfc_pass_lut.csv      # GSFC orbit/index to pass number lookup
├── tests/
│   ├── test_source_config.py               # YAML loading, config fields, cycle_index_key
│   ├── test_s3_bucket_enumerator.py        # S3 bucket enumeration + cycle index tests
│   ├── test_daily_file_job.py              # Source registry, job init, acquire phase
│   ├── test_cmr.py                         # S6 priority selection across collections
│   ├── test_gsfc_processing.py             # End-to-end GSFC processing with synthetic data
│   ├── test_s6_processing.py               # End-to-end S6 processing with synthetic data
│   ├── test_empty_templates.py             # Empty template schema validation
│   ├── test_smoothing.py                   # Smoothing filter edge cases
│   └── testing_granules/                   # Sample granules (not tracked in repo)
├── Dockerfile
├── requirements.txt
└── README.md
```

## Architecture

The code uses a plugin-style registry pattern. Each source is defined as a `SourcePipeline` — a bundle of four interchangeable components:

| Component     | Base class   | GSFC implementation | S6/S6B implementation | S3 bucket sources |
|---------------|-------------|---------------------|-----------------------|-------------------|
| **Enumerator** | `Enumerator` | `GSFCEnumerator` (single collection) | `S6Enumerator` (multi-collection priority selection) | `S3BucketEnumerator` (filename regex or cycle index) |
| **Downloader** | `Downloader` | `S3Downloader`     | `S3Downloader`        | IAM-based S3 access |
| **Ingestor**   | `Ingestor`  | `GSFCIngestor` (pass LUT, NOIB DAC) | `S6Ingestor` (grouped NetCDF) | Source-specific |
| **Processor**  | `DailyFile` | `GSFCDailyFile` (GSFC flag splitting) | `S6DailyFile` (S6 flag logic, MSS correction) | Source-specific |

The `SOURCE_REGISTRY` in `daily_file_job.py` maps source names to their `SourcePipeline`. To add a new satellite source, implement the four components and add a registry entry.

Processing runs in two phases:
1. **Acquire** — enumerate granules, download files, ingest into normalized `IngestedData`
2. **Process** — run the source-specific `DailyFile` subclass to produce the output dataset

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "source": "S6"
}
```

All three fields are required. Available sources are defined in `daily_files/config/sources.yaml`.

## Lambda output

```json
{
  "status": "success",
  "data": {
    "bucket": "my-bucket",
    "date": "2025-01-15",
    "source": "S6"
  }
}
```

## S3 paths

| Path | Description |
|------|-------------|
| `daily_files/p1/{source}/{year}/{source}_alt_ref_at_v1_{YYYYMMDD}.nc` | Output P1 daily file (write) |
| `aux_files/GSFC_NOIB/Merged_..._Cycle_{NNNN}.V5_2.nc` | GSFC NOIB cycle files for DAC computation (read, GSFC only) |

## Source configuration

Each source has settings at two levels:

**Global registry** (`utilities/sources.yaml`) — shared fields inherited by all stages: `product_type`, `unify`, `start_date`, `end_date`.

**Stage-local config** (`daily_files/config/sources.yaml`) — daily-files-specific fields merged with the global registry:

| Field               | Description                                              |
|---------------------|----------------------------------------------------------|
| `filename_template` | Output filename pattern (e.g. `{source}_alt_ref_at_v1_{date}.nc`) |
| `s3_prefix`         | S3 key prefix for output files                           |
| `source_mss`        | Source mean sea surface (e.g. DTU15, DTU18)              |
| `target_mss`        | Target mean sea surface (DTU21)                          |
| `mss_diff_file`     | MSS difference grid filename                             |
| `empty_template`    | Empty NetCDF template filename                           |
| `smoothing`         | Filter parameters: `speed` (km/s) and `sigma` (km)      |
| `collections`       | CMR collection(s): `shortname`, `concept_id`, `priority`, `source_label`, `source_url`, `reference` |
| `source_bucket`     | *(S3 bucket sources only)* S3 bucket containing source files |
| `source_prefix_pattern` | *(S3 bucket sources only)* S3 prefix pattern with `{source}`, `{year}` placeholders |
| `source_filename_pattern` | *(S3 bucket sources only)* Filename pattern with `{source}`, `{date8}` placeholders |
| `cycle_index_key`   | *(S3 bucket sources only, optional)* S3 key to a JSON file mapping cycle filenames to `{"start", "end"}` date ranges. When set, the enumerator uses the index to find files whose date range overlaps the target date instead of matching filenames by date. |

Current sources:

| Source | Collections | Source MSS | Target MSS |
|--------|------------|------------|------------|
| GSFC   | `MERGED_TP_J1_OSTM_OST_CYCLES_V52` | DTU15 | DTU21 |
| S6     | `JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_G01`, `..._NTC_G01_UNVALIDATED`, `..._STC_F` | DTU18 | DTU21 |
| S6B    | `JASON_CS_S6B_L2_ALT_LR_RED_OST_STC_G` | DTU18 | DTU21 |

To add a new source, add an entry to `utilities/sources.yaml` first, then add the stage-specific entry to `daily_files/config/sources.yaml`, implement the required components (enumerator, ingestor, processor), and register them in `SOURCE_REGISTRY`.

## Step Function

Defined in `state_machines/daily_file.asl.json`. First invokes a PODAAC credentials update Lambda, then uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `daily_files` Lambda for each date. Results are written to `pipeline_runs/results/daily_file/` in S3.

## Running tests

From the `daily_files/` directory:

```bash
source .venv/bin/activate  # or use the devcontainer
python -m unittest discover -s tests -t . -v
```

Note: `test_gsfc_processing` and `test_s6_processing` require sample granules in `tests/testing_granules/` (not tracked in the repo). See `tests/README.md` for details. All other tests use synthetic data or mocks.

## Dependencies

Key libraries (see `requirements.txt`):

- `xarray` / `netCDF4` / `h5netcdf` / `h5py` — reading and writing NetCDFs
- `numpy` / `pandas` / `scipy` — numerical computation
- `geopandas` / `shapely` / `pyproj` — basin polygon mapping
- `python-cmr` — CMR granule queries
- `boto3` / `s3fs` — AWS S3 access
- `pyyaml` — source config loading
