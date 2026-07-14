# Indicators

Computes climate indicators from simple grid files: Global Mean Sea Level (GMSL), ENSO, Pacific Decadal Oscillation (PDO), and Indian Ocean Dipole (IOD). Removes trends and seasonal cycles, fits climate patterns via least-squares regression, merges results with a cached indicator history, and uploads NetCDF, text, and metadata files to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

The Lambda reads a jobs manifest from S3, constructs S3 keys for each simple grid, then:

1. **Loads cached indicators** from `s3://{bucket}/indicators/{source}/indicators.nc` (if present). Supports legacy `gmsl` field renamed to `raw_gmsl`.
2. **Processes each simple grid** (skipping dates before 1993 and grids with <90% data coverage):
   - **Computes raw GMSL** — area-weighted mean of SSHA within ±66 latitude, converted to cm.
   - **Detrends and deseasons** — removes a linear trend (BH offset + slope) and monthly seasonal cycle from the grid.
   - **Fits climate patterns** — performs least-squares regression against ENSO, PDO, and IOD spatial patterns to produce scalar indicator values.
3. **Merges** new results with cached records, deduplicating by time (new values overwrite cached at the same time point).
4. **Normalizes GMSL** — subtracts the 1993 mean to produce zero-referenced `gmsl`.
5. **Computes smoothed GMSL** — 60-day running mean (±28.1-day window).
6. **Uploads outputs** to S3:
   - `indicators.nc` — full NetCDF with all indicators
   - Text files — one per indicator (`NASA_SSH_GMSL_INDICATOR.txt`, etc.) generated from templates
   - `.mp` metadata files — JSON with granule info, bounding box, and MD5 checksum
   - Archival versions — timestamped copies of text and `.mp` files under `archive/{INDICATOR}/`

## Directory structure

```
indicators/
├── app.py                              # Lambda handler (entry point) + build_sg_key()
├── indicators/
│   ├── compute_indicators.py           # IndicatorProcessor class (processing, caching, upload)
│   ├── pattern_data.py                 # Pattern class (loads ENSO/PDO/IOD spatial patterns)
│   └── utils.py                        # Helpers: decimal year conversion, text/mp file generation
├── ref_files/
│   ├── BH_offset_and_trend_v0_new_grid.nc  # Trend + offset for detrending
│   ├── half_deg_grid_cell_areas.nc         # Grid cell areas for GMSL weighting
│   ├── ann_pattern.nc                      # Monthly seasonal cycle
│   ├── enso_pattern_and_index.nc           # ENSO spatial pattern
│   ├── pdo_pattern_and_index.nc            # PDO spatial pattern
│   ├── iod_pattern_and_index.nc            # IOD spatial pattern
│   └── txt_templates/
│       ├── README.md
│       ├── NASA_SSH_GMSL_INDICATOR.txt     # Header template for GMSL text output
│       ├── NASA_SSH_ENSO_INDICATOR.txt     # Header template for ENSO text output
│       ├── NASA_SSH_IOD_INDICATOR.txt      # Header template for IOD text output
│       └── NASA_SSH_PDO_INDICATOR.txt      # Header template for PDO text output
├── tests/
│   ├── __init__.py
│   └── test_indicator.py              # Unit tests
├── Dockerfile
└── README.md
```

## Lambda input

Unlike the other stages in this pipeline, indicators receives the entire jobs manifest rather than a single date. It is invoked directly as a Lambda Task (not via a Distributed Map).

```json
{
  "bucket": "my-bucket",
  "jobs_key": "pipeline_runs/run_id/source/sg_jobs.json",
  "source": "S6"
}
```

All three fields are required. The manifest at `jobs_key` is a JSON array of objects, each containing at least a `date` field:

```json
[
  {"date": "2025-01-15"},
  {"date": "2025-01-22"}
]
```

## Lambda output

```json
{
  "status": "success"
}
```

On error, raises an exception with a JSON body containing `status`, `errorType`, `errorMessage`, and the original `input`.

## S3 paths

| Path | Description |
|------|-------------|
| `{jobs_key}` | Jobs manifest (read) |
| `simple_grids/{source}/{year}/{source}_alt_ref_simple_grid_v1_1_{YYYYMMDD}.nc` | Input simple grids (read) |
| `indicators/{source}/indicators.nc` | Cached/output indicator NetCDF (read + write) |
| `indicators/{source}/NASA_SSH_{INDICATOR}_INDICATOR.txt` | Latest indicator text file (write) |
| `indicators/{source}/NASA_SSH_{INDICATOR}_INDICATOR.mp` | Latest metadata file (write) |
| `indicators/{source}/archive/{INDICATOR}/NASA_SSH_{INDICATOR}_INDICATOR_{YYYYMMDD}.txt` | Archival text file (write) |
| `indicators/{source}/archive/{INDICATOR}/NASA_SSH_{INDICATOR}_INDICATOR_{YYYYMMDD}.mp` | Archival metadata file (write) |

## Step Function

Invoked as the final step in the `simple_grid_pipeline` orchestration defined in `state_machines/simple_grid_pipeline.asl.json`. Runs after both Simple Grids and ENSO stages complete. Unlike those stages, indicators is invoked as a single direct Lambda Task (not a Distributed Map), since it processes all dates in one invocation and manages its own caching.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh indicators
```

### Test coverage

| Test class | What it tests |
|------------|---------------|
| `TestBuildSgKey` | S3 key construction for different sources, dates, and year boundaries |
| `TestMergeIndicators` | Empty cache/new, overwrite at same time point, append new times, sorted output |
| `TestGenerateDs` | 1993 GMSL normalization, raw_gmsl preservation, missing-1993 guard, smoothed_gmsl presence, time sorting |

## Dependencies

Key libraries (see the `indicators` and `pipeline_runtime` extras in the root `pyproject.toml`):

- `numpy` / `xarray` / `pandas` — numerical computation, dataset handling, and DataFrames
- `netCDF4` / `h5py` / `h5netcdf` — reading and writing NetCDF files
- `pyresample` — `check_and_wrap` for coordinate handling
- `pyproj` — coordinate reference system handling
- `boto3` / `s3fs` — AWS S3 access (via `utilities.aws_utils`)
