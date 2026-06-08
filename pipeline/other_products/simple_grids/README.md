# Simple Grids

Aggregates 10-day windows of level-3 (P3) along-track daily files into gridded sea surface height anomaly (SSHA) products. Uses basin-aware Gaussian spatial resampling via pyresample, producing CF-1.7 compliant NetCDF files at half-degree (default) or quarter-degree resolution.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

For each processing date, the Lambda:

1. **Parses the event** — extracts `date` (center of the 10-day window), `source`, `bucket`, and optional `resolution`.
2. **Generates S3 keys** for P3 daily files spanning a 10-day window (center date ±5 days) using the source-specific filename prefix from `utilities/source_profile.py`.
3. **Streams daily files** from S3, merging them into a single dataset sorted by time. Renames legacy `ssh_smoothed`/`ssh` fields to `ssha_smoothed`/`ssha` if needed.
4. **Validates data coverage** — requires at least 150,000 valid `ssha_smoothed` data points; produces an empty grid if below threshold.
5. **Performs basin-aware Gaussian resampling** — iterates over basin regions using a connection table, resampling source observations onto the target grid with pyresample's `resample_gauss` (ROI = 600 km, sigma = 175 km, max 500 neighbours).
6. **Builds the output dataset** — includes `ssha`, `basin_flag`, `counts`, `time`, `basin_names_table`, and full CF-1.7 global attributes (DOI, institution, gridding method, etc.).
7. **Saves and uploads** the NetCDF to S3, with compressed encoding for all data variables.

## Directory structure

```
simple_grids/
├── app.py                              # Lambda handler (entry point)
├── simple_gridder/
│   ├── __init__.py
│   ├── gridder.py                      # SimpleGridderJob class (date windowing, S3 I/O, encoding)
│   ├── gridding.py                     # Gridder, Source, Target classes (resampling logic)
│   └── ref_files/
│       ├── basin_connection_table.txt   # Basin adjacency rules for resampling
│       ├── new_basin_mask_halfdeg.nc    # Half-degree basin mask
│       ├── new_basin_mask_quartdeg.nc   # Quarter-degree basin mask
│       └── basin/
│           ├── new_basin_lake_polygons.shp  # Basin names/IDs (shapefile)
│           ├── new_basin_lake_polygons.dbf
│           ├── new_basin_lake_polygons.shx
│           └── new_basin_lake_polygons.prj
├── Dockerfile
├── requirements.txt
├── .dockerignore
└── README.md
```

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "source": "S6",
  "resolution": null
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `bucket` | Yes | S3 bucket name |
| `date` | Yes | Center date of the 10-day window (`%Y-%m-%d`) |
| `source` | No | Source identifier (e.g., `S6`, `GSFC`). Defaults to `NASA-SSH` if omitted. |
| `resolution` | No | Set to `"quart"` for quarter-degree output. Omit or `null` for half-degree. |

## Lambda output

A **Job outcome** (`JobOutcome.to_dict()`). On a normal run it declares the grid key:

```json
{
  "schema_version": 1,
  "stage": "simple_grids",
  "status": "success",
  "date": "2025-01-15",
  "source": "NASA-SSH",
  "outputs": [
    {"key": "simple_grids/NASA-SSH/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250115.nc", "kind": "simple_grid"}
  ],
  "metadata": {}
}
```

When the 10-day window has no daily files to grid, it returns `status: "skipped"` with
`metadata.skip_reason` instead of producing nothing silently — `run_summary` lists the date
under `missing` with that reason rather than as an unexplained gap. On error it raises an
exception with a JSON body containing `status`, `errorType`, `errorMessage`, and the original
`input` (the failure path).

## S3 paths

| Path | Description |
|------|-------------|
| `daily_files/p3/{source}/{year}/{prefix}_{YYYYMMDD}.nc` | Input P3 daily files (read, 10-day window) |
| `simple_grids/{source}/{year}/{source}_alt_ref_simple_grid_v1_1_{YYYYMMDD}.nc` | Output half-degree grid (write) |
| `simple_grids/quart_deg/{source}/{year}/{source}_alt_ref_simple_grid_v1_1_quart_{YYYYMMDD}.nc` | Output quarter-degree grid (write, when `resolution="quart"`) |

Filename prefix for daily files is determined by the global source registry (`utilities/source_profile.py`).

## Step Function

Part of the `simple_grid_pipeline` orchestration defined in `state_machines/simple_grid_pipeline.asl.json`:

1. **Set SG Jobs** — filters the manifest and prepares the jobs list
2. **Simple Grids Execution** — Distributed Map (`state_machines/simple_grid.asl.json`, max concurrency 500) that invokes the `simple_grids` Lambda for each date
3. **ENSO Execution** — downstream stage
4. **Indicators** — downstream stage

The Map's invoke task unwraps the Lambda result (`Output: {% $states.result.Payload %}`) so the **Job outcome** is what the `ResultWriter` persists under `pipeline_runs/{source}/{run_id}/results/simple_grids/` for `run_summary` to read.

## Running tests

From the `simple_grids/` directory:

```bash
python -m unittest discover -s tests -t . -v
```

`tests/test_simple_grids.py` covers the handler's Job-outcome shape (success and the
no-daily-files skip). The job is also wired into CI (`.github/workflows/tests.yml`).

## Dependencies

Key libraries (see `requirements.txt`):

- `pyresample` — Gaussian spatial resampling with basin-aware ROI
- `numpy` / `xarray` / `scipy` / `dask` — numerical computation and lazy dataset merging
- `netcdf4` / `h5py` / `h5netcdf` — reading and writing NetCDF files
- `geopandas` / `pyproj` — shapefile I/O for basin polygon names
- `boto3` / `s3fs` — AWS S3 access
