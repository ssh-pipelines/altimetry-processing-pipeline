# Indicators

Computes climate indicators from simple grid files: Global Mean Sea Level (GMSL), ENSO, Pacific Decadal Oscillation (PDO), and Indian Ocean Dipole (IOD). Removes trends and seasonal cycles, fits climate patterns via least-squares regression, merges results with a cached indicator history, and uploads NetCDF, text, and metadata files to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

The Lambda reads a jobs manifest from S3, constructs S3 keys for each simple grid, then:

1. **Loads cached indicators** from `s3://{bucket}/products/{published_product}/indicators/indicators.nc` (if present). Supports legacy `gmsl` field renamed to `raw_gmsl`.
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

## Reference files

Bundled under `ref_files/`:

| File | Purpose |
|------|---------|
| `BH_offset_and_trend_v0_new_grid.nc` | Offset + trend grid used for detrending |
| `half_deg_grid_cell_areas.nc` | Grid cell areas for area-weighted GMSL |
| `ann_pattern.nc` | Monthly seasonal cycle |
| `enso_pattern_and_index.nc` | ENSO spatial pattern and index |
| `pdo_pattern_and_index.nc` | PDO spatial pattern and index |
| `iod_pattern_and_index.nc` | IOD spatial pattern and index |
| `txt_templates/NASA_SSH_{GMSL,ENSO,PDO,IOD}_INDICATOR.txt` | Header templates for the text outputs |

The spatial patterns are loaded by the `Pattern` class (`indicators/pattern_data.py`);
`IndicatorProcessor` (`indicators/compute_indicators.py`) owns processing, caching and upload.

## Lambda input

Unlike the other stages in this pipeline, indicators receives the entire jobs manifest rather than a single date. It is invoked directly as a Lambda Task (not via a Distributed Map).

```json
{
  "bucket": "my-bucket",
  "jobs_key": "pipeline_runs/S6/20250115T120000/NASA-SSH/sg_jobs.json",
  "published_product": "NASA-SSH"
}
```

All three fields are required. Indicators are keyed on a **published product**,
not a **Source**: they derive from the reference-mission grid, which several
missions contribute to. The manifest at `jobs_key` is a JSON array of objects, each containing at least a `date` field:

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
| `simple_grids/{published_product}/{year}/*_simple_grid_v1_1_{YYYYMMDD}.nc` | Input simple grids (read — the staged copy, see below) |
| `products/{published_product}/indicators/indicators.nc` | Cached/output indicator NetCDF (read + write) |
| `products/{published_product}/indicators/NASA_SSH_{INDICATOR}_INDICATOR.txt` | Latest indicator text file (write) |
| `products/{published_product}/indicators/NASA_SSH_{INDICATOR}_INDICATOR.mp` | Latest metadata file (write) |
| `products/{published_product}/indicators/archive/{INDICATOR}/NASA_SSH_{INDICATOR}_INDICATOR_{YYYYMMDD}.txt` | Archival text file (write) |
| `products/{published_product}/indicators/archive/{INDICATOR}/NASA_SSH_{INDICATOR}_INDICATOR_{YYYYMMDD}.mp` | Archival metadata file (write) |

**Indicators write through to `products/` rather than being promoted.** Nothing
outside this stage reads them, and `indicators.nc` is self-read as a cache — so
the published copy *is* the cache, and a staged copy plus a promotion step would
buy nothing. This is the one stage for which the published area is read-write
(see [ADR 0008](../../../docs/adr/0008-published-products-prefix.md)).

The input grids are read from **staging**, not from `products/`: the grid has to
be staged for ENSO anyway, so there is no reason to take the long way round.

The `.txt` and `.mp` filenames are externally fixed. `generate_mp` emits a
PO.DAAC ingest sidecar whose `collection` shortname is derived from the filename,
so indicators can *move* prefix — they cannot be renamed without coordinating
with PO.DAAC.

**Seeding note.** `load_cached_indicators` treats a missing key as "starting
fresh", so against an empty published prefix the first run would compute only the
Mondays in the current manifest, silently skip the 1993 normalization, and write
a valid-looking GMSL product missing thirty years to a PO.DAAC-facing filename —
from a *successful* run. Copying the existing indicators history into the new
prefix before the first run is mandatory, not tidiness.

## Step Function

Invoked as the final step in the `sg_pipeline` orchestration defined in `state_machines/sg_pipeline.asl.json`. Runs after both Simple Grids and ENSO stages complete. Unlike those stages, indicators is invoked as a single direct Lambda Task (not a Distributed Map), since it processes all dates in one invocation and manages its own caching.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh indicators
```

### Test coverage

| Test class | What it tests |
|------------|---------------|
| `TestBuildSgKey` | S3 key construction for the staged grid, year boundaries, and the refusal to resolve a Source name as a published product |
| `TestMergeIndicators` | Empty cache/new, overwrite at same time point, append new times, sorted output |
| `TestGenerateDs` | 1993 GMSL normalization, raw_gmsl preservation, missing-1993 guard, smoothed_gmsl presence, time sorting |

## Dependencies

Key libraries (see the `indicators` and `pipeline_runtime` extras in the root `pyproject.toml`):

- `numpy` / `xarray` / `pandas` — numerical computation, dataset handling, and DataFrames
- `netCDF4` / `h5py` / `h5netcdf` — reading and writing NetCDF files
- `pyresample` — `check_and_wrap` for coordinate handling
- `pyproj` — coordinate reference system handling
- `boto3` / `s3fs` — AWS S3 access (via `utilities.aws_utils`)
