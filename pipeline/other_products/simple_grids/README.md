# Simple Grids

Aggregates 10-day windows of **published** along-track daily files into gridded sea surface height anomaly (SSHA) products. Uses basin-aware Gaussian spatial resampling via pyresample, producing CF-1.7 compliant NetCDF files at half-degree (default) or quarter-degree resolution.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

For each processing date, the Lambda:

1. **Parses the event** — extracts `date` (center of the 10-day window), `contributors`, `publish_as`, `bucket`, and optional `resolution`.
2. **Generates S3 keys** for each contributor's published along-track files spanning a 10-day window (center date ±5 days), via `published_along_track_key`.
3. **Streams daily files** from S3, merging them into a single dataset sorted by time. Renames legacy `ssh_smoothed`/`ssh` fields to `ssha_smoothed`/`ssha` if needed.
4. **Validates data coverage** — requires at least 150,000 valid `ssha_smoothed` data points; produces an empty grid if below threshold.
5. **Performs basin-aware Gaussian resampling** — iterates over basin regions using a connection table, resampling source observations onto the target grid with pyresample's `resample_gauss` (ROI = 600 km, sigma = 175 km, max 500 neighbours).
6. **Builds the output dataset** — includes `ssha`, `basin_flag`, `counts`, `time`, `basin_names_table`, and full CF-1.7 global attributes (DOI, institution, gridding method, etc.).
7. **Saves and uploads** the NetCDF to S3, with compressed encoding for all data variables.

## Reference files

Bundled under `simple_gridder/ref_files/`:

| File | Purpose |
|------|---------|
| `basin_connection_table_v2.txt` | Basin adjacency rules — which basins may share data when resampling |
| `new_basin_mask_halfdeg.nc` | Half-degree basin mask (the output grid) |
| `new_basin_mask_quartdeg.nc` | Quarter-degree basin mask |
| `basin/new_basin_lake_polygons.{shp,dbf,shx,prj}` | Basin names/IDs as a shapefile |

`SimpleGridderJob` (`simple_gridder/gridder.py`) owns date windowing, S3 I/O and encoding;
the `Gridder`/`Source`/`Target` classes in `simple_gridder/gridding.py` hold the resampling logic.

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "contributors": ["NASA-SSH"],
  "publish_as": "NASA-SSH",
  "resolution": null
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `bucket` | Yes | S3 bucket name |
| `date` | Yes | Center date of the 10-day window (`%Y-%m-%d`) |
| `contributors` | Yes | The **published products** to read. Always published, never staging. |
| `publish_as` | Yes (may be `null`) | The published product this grid *becomes*, or `null` for development output |
| `resolution` | No | Set to `"quart"` for quarter-degree output. Omit or `null` for half-degree. |

**Two independent inputs, where there used to be one `source` doing both jobs.**
What the grid reads and what it becomes are separate facts: `contributors`
decouples gridding from **Source** entirely, and `publish_as` alone decides
whether the result is a product. Officialness is never inferred from the
contributor set — set-equality against a derived expectation fails silently in
both directions, and a diverged set would route a real product into the
development directory on a *successful* run.

`publish_as` must be present. Absent and `null` are different: a `null`
`publish_as` is the deliberate "development output" choice, so it cannot be
defaulted.

```
production:   contributors=["NASA-SSH"], publish_as="NASA-SSH"
development:  contributors=[...],        publish_as=null
```

Today the contributor list has exactly one live value. The seam ships anyway: it
is what decouples the stage from Source, and it makes development combinations
expressible now. A composite grid over the reference line plus every
high-latitude line is designed for and deferred — the 15 high-latitude sources
have disjoint operating spans, so on most dates most contribute nothing, and
`fetch_daily_files` already logs and skips missing keys.

**Out of scope, deliberately:** gridding a source that has never been published.
Contributors are published products, full stop. Since a source publishes from its
first production run, the excluded case is really "a satellite mid-integration,
`enabled: false`" — handled by invoking this Lambda manually or running it
offline, not by a second code path through the stage.

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
  "metadata": {"contributors": ["NASA-SSH"]}
}
```

`source` is `publish_as`, or the literal `"development"` for a grid with no
`publish_as` — a development grid is not keyed on any product.

When the 10-day window has no daily files to grid, it returns `status: "skipped"` with
`metadata.skip_reason` instead of producing nothing silently — `run_summary` lists the date
under `missing` with that reason rather than as an unexplained gap. On error it raises an
exception with a JSON body containing `status`, `errorType`, `errorMessage`, and the original
`input` (the failure path).

## S3 paths

| Path | Description |
|------|-------------|
| `products/{contributor}/along_track/{year}/{contributor}_*.nc` | Input published along-track files (read, 10-day window, one set per contributor) |
| `simple_grids/{publish_as}/{year}/{publish_as}_alt_ref_simple_grid_v1_1_{YYYYMMDD}.nc` | Staged output (write, when `publish_as` is set) |
| `simple_grids/development/{identity}/{year}/{identity}_dev_simple_grid_v1_1_{YYYYMMDD}.nc` | Development output (write, when `publish_as` is `null`); `identity` is the contributor list joined with `_` |
| `simple_grids/quart_deg/{publish_as}/{year}/{publish_as}_alt_ref_simple_grid_quart_v1_1_{YYYYMMDD}.nc` | Quarter-degree output (write, when `resolution="quart"`) — a known one-off that sits outside the layout's key families |

The stage writes to **staging**, not to `products/`; the `publisher` promotes the
result when `publish_as` is set. The staged copy stays, because ENSO and
indicators read it.

The output's in-file identity comes from `publish_as`, not from the contributors.
With `publish_as` set, the Product is whatever `published_products:{publish_as}.simple_grid`
declares — which is precisely the fact no source can supply, since a composite
grid is named after no source. With `publish_as: null` the Product is
`simple_grid_development`, *regardless of contributors*: deriving it from the
contributors would mean a development grid over `["NASA-SSH"]` writing the real
reference DOI into a throwaway file. Its `id: "TBD"`, its title saying so, and its
`_dev_` filename family are all part of the same rule — a development grid cannot
be mistaken for a product in the file any more than in the path.

`gridding.py` writes the CF global attributes that every NASA-SSH product shares
(`institution`, `project`, `publisher_*`, `creator_*`, the gridding method, the
time coverage) and merges the resolved product's own identity — `title`,
`summary`, `id`, `processing_level`, `product_short_name`, `product_version`,
`references`, `source_url` — over them from `utilities/products.yaml`.

`simple_grid_high_latitude` is now referenced by nothing: no published identity
declares it, and the development path resolves elsewhere. It is reserved for the
deferred composite grid, which will claim or replace it; it has never produced a
file, so its name and template are free to reshape.

## Step Function

Part of the `sg_pipeline` orchestration defined in `state_machines/sg_pipeline.asl.json`:

1. **Set SG Jobs** — filters the manifest and prepares the jobs list
2. **Simple Grids Execution** — Distributed Map (`state_machines/simple_grids.asl.json`, max concurrency 500) that invokes the `simple_grids` Lambda for each date. Its `ItemSelector` composes `contributors` / `publish_as` from the run's `published_product`.
3. **Publish Simple Grids** — the `publisher`, promoting each staged grid to `products/`
4. **ENSO Execution** — downstream stage, reading the staged grid
5. **Indicators** — downstream stage, reading the staged grid

The Map's invoke task unwraps the Lambda result (`Output: {% $states.result.Payload %}`) so the **Job outcome** is what the `ResultWriter` persists under `pipeline_runs/{source}/{run_id}/results/simple_grids/` for `run_summary` to read.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh simple_grids
```

`tests/test_simple_grids.py` covers the handler's Job-outcome shape (success and the
no-daily-files skip), key/window derivation, the coverage guard, the basin-connection parser, and
the per-product in-file identity. The job is also wired into CI (`.github/workflows/tests.yml`).

## Dependencies

Key libraries (see the `simple_grids` and `pipeline_runtime` extras in the root `pyproject.toml`):

- `pyresample` — Gaussian spatial resampling with basin-aware ROI
- `numpy` / `xarray` / `scipy` / `dask` — numerical computation and lazy dataset merging
- `netcdf4` / `h5py` / `h5netcdf` — reading and writing NetCDF files
- `geopandas` / `pyproj` — shapefile I/O for basin polygon names
- `boto3` / `s3fs` — AWS S3 access
