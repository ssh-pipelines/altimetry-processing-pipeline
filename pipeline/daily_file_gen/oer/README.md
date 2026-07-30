# Orbit Error Reduction (OER)

Fits a cubic-spline polygon to crossover SSH differences and applies the resulting correction to a processing-level-1 daily file, producing a level-2 daily file with reduced orbit error. Runs as an AWS Lambda function, invoked once per date via a Distributed Map in the `oer.asl.json` Step Function.

## Crossover types

OER handles two crossover geometries, dispatched from the source's `product_type` (see [`utilities/source_profile.py`](../../../utilities/source_profile.py)):

| `product_type` | `crossover_type` | Difference | trackid / stacking | Fetch window |
|----------------|------------------|------------|--------------------|--------------|
| `reference` (e.g. S6, GSFC) | `self` | `dssh = ssh1 - ssh2` between two observations of the **same** satellite | Both sides contribute: `+dssh` at pass 1 and `-dssh` at pass 2, keyed by both trackids (shared orbit error) | Backward-looking (10 days back + 1 pad) |
| `high_latitude` (e.g. S3B) | `reference` | `dssh = ssh1 - ssh2` where `ssh2` is the fixed, finalized reference-mission truth | Single sample per crossover at `time1`, keyed by the high-lat trackid only (**no** sign-flip stacking); reference schema has no `time2`/`cycle2` | Centered (±`reference_window_size` days) |

The mapping is `_CROSSOVER_TYPE_BY_PRODUCT_TYPE` in [`oer/oer.py`](oer/oer.py). Both paths share the same `oerfit` spline solver, correction evaluation, and application steps — only the pair-building and fetch window differ.

## Ground speed

`oerfit` places spline knots by converting each pass's knot time-span into an along-track distance, which requires the satellite's ground speed. This is a single canonical per-source value, `common.ground_speed` (km/s), shared with the daily-file smoothing filter. Compute it for a new source with [`tools/compute_ground_speed.py`](../../../tools/compute_ground_speed.py) and set it under `common:` in the source YAML. Sources without an explicit value fall back to the `SourceCommon` default (5.745).

## How it works

For each processing date, the Lambda:

1. **Fetches crossover files** from S3 under `crossovers/p1/{source}/{year}/`. The self path uses a backward-looking window (10 days back + 1 day pad); the reference path uses a centered window (±`reference_window_size` days, default 2). Raises if no crossover files are found.
2. **Creates a polygon** — extracts SSH crossover pairs (self-stacked or reference, per `crossover_type`), computes `ssh1 - ssh2` differences, filters to the target day with a 2-hour margin, discards differences with absolute value > 0.3 m, and fits a piecewise cubic spline via `oerfit` using the source's `ground_speed`. Returns zero coefficients when no data falls within the window. Uploads the polygon NetCDF to S3 (with a `crossover_type` attribute).
3. **Evaluates the correction** — evaluates the fitted spline (via `scipy.interpolate.PPoly`) at each daily-file time step and negates it to produce an additive OER correction. Uploads the correction NetCDF to S3.
4. **Applies the correction** — adds the OER correction to `ssha` and `ssha_smoothed`, attaches the `oer` variable to the daily file, sets `product_generation_step = "2"`, updates `history`, and appends a `processing_history` step (generation step 2, recording the correction source) to the in-file provenance trail (see [`utilities/provenance.py`](../../../utilities/provenance.py) and ADR 0005). Uploads the P2 daily file to S3 with zlib compression.

All intermediate and final NetCDFs are written to a temporary directory that is cleaned up after the run.

## Directory structure

```
oer/
├── app.py                              # Lambda handler (entry point)
├── oer/
│   ├── __init__.py
│   ├── oer.py                          # OerCorrection class: S3 I/O, pipeline orchestration, crossover-type dispatch
│   ├── compute_polygon_correction.py   # Spline fitting (self/reference pair-building), correction evaluation & application
│   ├── oerfit.py                       # Low-level cubic spline solver (La Traon & Ogor, 1998)
│   └── config/
│       └── source_config.py            # OerConfig (ground_speed, reference_window_size) loaded from source YAML
├── tests/
│   ├── __init__.py
│   ├── test_oer.py                     # Consistency, empty-input, apply-correction, and reference-polygon tests
│   ├── test_oerfit.py                  # Spline solver input validation, output shape, and ground_speed tests
│   ├── test_source_config.py           # OerConfig loading and product_type→crossover_type dispatch
│   └── sample_data/
│       ├── sample_inputs/              # 13 crossover files + 1 daily file (gzip-compressed)
│       └── sample_output/              # Reference polygon, correction, and daily file (gzip-compressed)
├── Dockerfile
└── README.md
```

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-01",
  "source": "S6"
}
```

All three fields are required.

## Lambda output

```json
{
  "status": "success",
  "data": {
    "bucket": "my-bucket",
    "date": "2025-01-01",
    "source": "S6"
  }
}
```

## S3 paths

| Path | Description |
|------|-------------|
| `crossovers/p1/{source}/{year}/xovers_{source}-{date}.nc` | Input crossover files (read) |
| `daily_files/p1/{source}/{year}/{prefix}_{YYYYMMDD}.nc` | Input P1 daily file (read) |
| `oer/{source}/{year}/oerpoly_{source}_{date}.nc` | Output polygon NetCDF (write) |
| `oer/{source}/{year}/oer_correction_{source}_{date}.nc` | Output correction NetCDF (write) |
| `daily_files/p2/{source}/{year}/{prefix}_{YYYYMMDD}.nc` | Output P2 daily file (write) |

Filename prefix is determined by the global source registry (`utilities/source_profile.py`): `{source}_alt_ref_at_v1_1` for reference products, `{source}_alt_hilat_at_v1_1` for high-latitude products.

## Step Function

Defined in `state_machines/oer.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `oer` Lambda for each date. Results are written to `pipeline_runs/results/oer/` in S3.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh oer
```

### Test data

Sample input/output files in `tests/sample_data/` are gzip-compressed to reduce repo size. The test suite automatically decompresses them to a temp directory at runtime.

### Test coverage

`test_oer.py`:

| Test class               | What it tests                                                           |
|--------------------------|-------------------------------------------------------------------------|
| `ConsistencyTestCase`    | Runs all 3 pipeline steps on sample data and compares every output field against known-good reference files (polygon, correction, P2 daily file) |
| `EmptyInputTestCase`     | Zero polynomial when no data falls in the day window, default tbrk range, zero RMS/nint |
| `ApplyCorrectionTestCase`| Mismatched time raises, empty time no mutation, OER added to ssha/ssha_smoothed |
| `ReferenceCrossoverPolygonTestCase` | Reference-schema xover (no `time2`/`cycle2`) does not KeyError; `_reference_pairs` uses `dssh = ssh1 - ssh2` with a single trackid and no sign-flip; `_self_pairs` still stacks with sign-flip |
| `OerCrossoverTypeDispatchTestCase` | `product_type` → `crossover_type` mapping (`reference`→self, `high_latitude`→reference) |

`test_oerfit.py`:

| Test class               | What it tests                                                           |
|--------------------------|-------------------------------------------------------------------------|
| `InputValidationTestCase`| Rejects mismatched ptime/dssh and dssh/trackid array sizes              |
| `OutputShapeTestCase`    | Coef has 4 rows, columns match intervals, tbrk sorted, RMS/nint shapes, nint total matches input |
| `GroundSpeedTestCase`    | Default reproduces the historical 5.7 result; a different `ground_speed` changes knot placement |

`test_source_config.py`:

| Test class          | What it tests                                                                |
|---------------------|------------------------------------------------------------------------------|
| `OerConfigTestCase` | `OerConfig` loads from source YAML, S6's canonical `ground_speed` (5.7529), default fallback (5.745), `reference_window_size`, and `product_type`→`crossover_type` dispatch |

Tolerances (science-meaningful, not machine epsilon): `1e-4` m absolute for coefficients, OER, and SSH — well below OER's cm-scale signal. `ConsistencyTestCase` fits with S6's canonical `ground_speed` (5.7529) to mirror production; the golden files were generated at the legacy 5.7, which on this sample data produces identical output (the 0.9% change crosses no `oerfit` knot threshold), and the relaxed tolerance keeps the test valid if a future sample regeneration does cross one.

## Dependencies

Key libraries (see the `oer` and `pipeline_runtime` extras in the root `pyproject.toml`):

- `xarray` / `netcdf4` / `h5netcdf` / `h5py` — reading and writing NetCDF files
- `numpy` / `scipy` — numerical computation and spline evaluation (`PPoly`)
- `boto3` / `s3fs` — AWS S3 access
- `dask` — lazy array backend for `xr.open_mfdataset`
