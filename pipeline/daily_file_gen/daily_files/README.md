# Daily Files

Generates level-1 (P1) along-track daily files from satellite altimeter data. For each processing date and source, downloads the granules `pipeline_init` discovered, ingests and harmonizes the raw data, applies quality flagging, MSS correction, smoothing, and basin mapping, then uploads the resulting NetCDF to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

For each processing date, the Lambda:

1. **Validates the source** against `utilities/sources/{source}.yaml` and `SOURCE_REGISTRY`.
2. **Downloads** the granules named in the event — from PODAAC's S3 bucket for CMR sources, the AVISO HTTP endpoint for THREDDS sources, or the source S3 bucket directly for `s3_bucket` sources. Granule *discovery* happens in `pipeline_init`; this stage consumes the URIs it is handed.
3. **Ingests** raw files into a normalized `IngestedData` structure (source-specific: extracts SSHA, lat/lon, time, cycle, pass, DAC, and any source-specific fields). For S6/S6B sources, an **orbit swap** is applied per pass file: a precise orbit file (POE for NTC granules, MOE for STC granules) is downloaded from JPL and passed to a C executable (`interpPosGoaToNetCDFtimes.e`) that recomputes SSHA using the improved orbit. If the orbit file cannot be fetched or the swap fails (wrong output length, non-zero exit code, timeout), the ingester falls back to the original `ssha_nr` values and logs a warning. Orbit files are cached in `/tmp/` by date and type so multiple passes on the same day share a single download.
4. **Processes** the ingested data into a daily file dataset:
   - Maps observations to geographic basins using basin shapefiles
   - Creates `nasa_flag` from source-specific quality flags and a rolling median filter
   - Subsets data to the target date and drops duplicate times
   - Normalizes the MSS reference to DTU21 — `reference` sources swap via a precomputed difference grid, `high_latitude` sources interpolate a bundled DTU21 grid at each granule's lon/lat ([ADR 0002](../../../docs/adr/0002-aviso-l2p-mss-handling.md))
   - Flags land/lake basins in `nasa_flag`
   - Computes `ssha_smoothed` using a 19-point Gaussian-like along-track filter, with `sigma` from the `daily_files:` config and the along-track speed from `common.ground_speed`
   - Sets variable and global CF-compliant metadata
5. **Validates** the output dataset against a schema (required global attributes, variables, and per-variable attributes).
6. **Appends a `processing_history` step** (generation step 1, recording the source files / granule count) before uploading — the first entry in the in-file provenance trail that OER and the finalizer extend (see [`utilities/provenance.py`](../../../utilities/provenance.py) and ADR 0005).
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
│   │   ├── downloader.py                   # S3Downloader / HttpDownloader (consume URI strings)
│   │   ├── aviso_auth.py                   # AVISO HTTP session builder
│   │   └── orbit_fetcher.py                # OrbitFetcher (downloads POE/MOE orbit files from JPL)
│   ├── ingestion/
│   │   ├── ingest.py                       # Abstract Ingestor + IngestedData dataclass
│   │   ├── gsfc_ingest.py                  # GSFCIngestor (pass LUT, DAC from NOIB cycles)
│   │   ├── s6_ingest.py                    # S6Ingestor (grouped NetCDF extraction + orbit swap)
│   │   └── orbit_swap.py                   # run_orbit_swap(): shells out to C executable, returns swapped SSHA
│   ├── processing/
│   │   ├── daily_file.py                   # Abstract DailyFile base class
│   │   ├── gsfc_daily_file.py              # GSFCDailyFile (GSFC flag splitting, manual outliers, bad_points)
│   │   ├── s6_daily_file.py                # S6DailyFile (S6 flag logic, MSS sol1/sol2 correction, bad_points)
│   │   └── smoothing.py                    # 19-point Gaussian-like SSHA smoothing filter
│   └── ref_files/
│       ├── empty_templates/                # Empty NetCDF templates per source
│       ├── mss_diffs/                      # MSS difference grids (DTU15/18 minus DTU21)
│       ├── basin/                          # Basin/lake polygon shapefiles
│       └── complete_gsfc_pass_lut.csv      # GSFC orbit/index to pass number lookup
├── tests/
│   ├── test_source_config.py               # YAML loading, config fields, cycle_index_key
│   ├── test_daily_file_job.py              # Source registry, job init, acquire phase
│   ├── test_gsfc_processing.py             # End-to-end GSFC processing with synthetic data
│   ├── test_s6_processing.py               # End-to-end S6 processing with synthetic data
│   ├── test_bad_points.py                  # bad_points config flagging
│   ├── test_empty_templates.py             # Empty template schema validation
│   ├── test_smoothing.py                   # Smoothing filter edge cases
├── Dockerfile
└── README.md
```

## Architecture

The code uses a plugin-style registry pattern. Each source is defined as a `SourcePipeline` — a bundle of three interchangeable components:

| Component      | Base class   | GSFC implementation                   | S6/S6B implementation                                       | S3 bucket / S3B                                 |
| -------------- | ------------ | ------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------- |
| **Downloader** | `Downloader` | `S3Downloader`                        | `S3Downloader`                                              | `S3Downloader` (IAM) / `HttpDownloader` (AVISO) |
| **Ingestor**   | `Ingestor`   | `GSFCIngestor` (pass LUT, NOIB DAC)   | `S6Ingestor` (grouped NetCDF + orbit swap via C executable) | Source-specific                                 |
| **Processor**  | `DailyFile`  | `GSFCDailyFile` (GSFC flag splitting) | `S6DailyFile` (S6 flag logic, MSS correction)               | Source-specific                                 |

Granule discovery happens upstream in `pipeline_init`, which writes a manifest of granule URIs per date. The `SOURCE_REGISTRY` in `daily_file_job.py` maps source names to their `SourcePipeline`. To add a new satellite source, implement the three components and add a registry entry.

Processing runs in two phases:

1. **Acquire** — download URIs from the manifest, ingest into normalized `IngestedData`
2. **Process** — run the source-specific `DailyFile` subclass to produce the output dataset

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "source": "S6",
  "granules": ["s3://podaac-ops-cumulus-protected/.../S6A_..._F09.nc"]
}
```

All four fields are required. Available sources are those with a `daily_files:` section in `utilities/sources/{source}.yaml` **and** an entry in `SOURCE_REGISTRY`. The `granules` list is produced by `pipeline_init` and consumed verbatim — no upstream discovery happens here.

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

| Path                                                                    | Description                                                 |
| ----------------------------------------------------------------------- | ----------------------------------------------------------- |
| `daily_files/p1/{source}/{year}/{source}_alt_ref_at_v1_1_{YYYYMMDD}.nc` | Output P1 daily file, `reference` product type (write)       |
| `daily_files/p1/{source}/{year}/{source}_alt_hilat_at_v1_1_{YYYYMMDD}.nc` | Output P1 daily file, `high_latitude` product type (write) |
| `aux_files/GSFC_NOIB/Merged_..._Cycle_{NNNN}.V5_2.nc`                   | GSFC NOIB cycle files for DAC computation (read, GSFC only) |

Both come from `utilities.pipeline_layout`; the filename family follows the source's `product_type`.

## Source configuration

One file per source, `utilities/sources/{source}.yaml`, holding a `common:` block plus one section per stage. `get_source_config` merges `common` with the `daily_files:` section into a `SourceConfig`.

The `daily_files:` section — this stage's own fields:

| Field                     | Description                                                                                                                                                                                                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `source_mss`              | _(`reference` product type only, required)_ Source mean sea surface (e.g. DTU15, DTU18)                                                                                                                                                                                        |
| `target_mss`              | _(`reference` only, required)_ Target mean sea surface (DTU21)                                                                                                                                                                                                                 |
| `mss_diff_file`           | _(`reference` only, required)_ MSS difference grid filename                                                                                                                                                                                                                    |
| `smoothing`               | Filter parameter: `sigma` (km). The along-track speed is **not** set here — it comes from `common.ground_speed`, which the OER stage shares.                                                                                                                                    |
| `bad_points`              | _(optional)_ Map of ISO date strings to lists of `{time: <ISO datetime>}` entries. Any observation whose timestamp matches a listed time (at second precision) will have `nasa_flag` forced to 1, regardless of other quality criteria. Supported for GSFC and S6/S6B sources. |

`SourceConfig.__post_init__` enforces the MSS fields by product type: a `reference` source **must** set all three, and a `high_latitude` source **must not** set any of them — high-latitude processors interpolate DTU21 directly from a bundled grid instead of swapping via a diff file ([ADR 0002](../../../docs/adr/0002-aviso-l2p-mss-handling.md)).

Relevant `common:` fields (shared with other stages): `product_type`, `discovery_type`, `ground_speed`, `collections`, and — for `s3_bucket` discovery — `source_bucket`, `source_prefix_pattern`, `source_filename_pattern`, `cycle_index_key`.

Output prefixes and filenames are **not** configured per source. They are derived from `utilities.pipeline_layout` and `utilities/products.yaml`, which own the version and filename template per product family. Empty-file templates are built in code (`processing/empty_template.py`), not read from config.

Current sources (those with a `daily_files:` section):

| Source | Product type   | Collections                                                                      | MSS handling        |
| ------ | -------------- | -------------------------------------------------------------------------------- | ------------------- |
| GSFC   | reference      | `MERGED_TP_J1_OSTM_OST_CYCLES_V61`                                               | DTU15 → DTU21 (diff file) |
| S6     | reference      | `JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_G01`, `..._NTC_G01_UNVALIDATED`, `..._STC_F` | DTU18 → DTU21 (diff file) |
| S6B    | reference      | `JASON_CS_S6B_L2_ALT_LR_RED_OST_STC_G`                                           | DTU18 → DTU21 (diff file) |
| S3B    | high_latitude  | `dataset-l2p-uncross-calibrated-ntc-sla-s3b-1hz` (THREDDS)                        | DTU21 interpolated  |
| EXAMPLE_S3 | reference  | — (template, not a real source)                                                  | DTU15 → DTU21 (diff file) |

To add a new source, create `utilities/sources/{source}.yaml` with a `common` block and a `daily_files:` section, implement the required components (downloader, ingestor, processor), and register them in `SOURCE_REGISTRY` (`daily_files/daily_file_job.py`).

## Step Function

Defined in `state_machines/daily_file.asl.json`. First invokes a PODAAC credentials update Lambda, then uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `daily_files` Lambda for each date. Results are written to `pipeline_runs/results/daily_file/` in S3.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh daily_files
```

## Dependencies

Key libraries (see the `daily_files` and `pipeline_runtime` extras in the root `pyproject.toml`):

- `xarray` / `netCDF4` / `h5netcdf` / `h5py` — reading and writing NetCDFs
- `numpy` / `pandas` / `scipy` — numerical computation
- `geopandas` / `shapely` / `pyproj` — basin polygon mapping
- `python-cmr` — CMR granule queries
- `boto3` / `s3fs` — AWS S3 access
- `pyyaml` — source config loading
