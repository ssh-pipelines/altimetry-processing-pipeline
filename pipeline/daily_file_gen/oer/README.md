# Orbit Error Reduction (OER)

Fits a cubic-spline polygon to self-crossover SSH differences and applies the resulting correction to a processing-level-1 daily file, producing a level-2 daily file with reduced orbit error. Runs as an AWS Lambda function, invoked once per date via a Distributed Map in the `oer.asl.json` Step Function.

## How it works

For each processing date, the Lambda:

1. **Fetches crossover files** from S3 for a sliding window (10 days back + 1 day pad on each side) under `crossovers/p1/{source}/{year}/`. Raises if no crossover files are found.
2. **Creates a polygon** — extracts SSH crossover pairs, computes `ssh1 - ssh2` differences, filters to the target day with a 2-hour margin, discards differences with absolute value > 0.3 m, and fits a piecewise cubic spline via `oerfit`. Returns zero coefficients when no data falls within the window. Uploads the polygon NetCDF to S3.
3. **Evaluates the correction** — evaluates the fitted spline (via `scipy.interpolate.PPoly`) at each daily-file time step and negates it to produce an additive OER correction. Uploads the correction NetCDF to S3.
4. **Applies the correction** — adds the OER correction to `ssha` and `ssha_smoothed`, attaches the `oer` variable to the daily file, sets `product_generation_step = "2"`, updates `history`, and appends a `processing_history` step (generation step 2, recording the correction source) to the in-file provenance trail (see [`utilities/provenance.py`](../../../utilities/provenance.py) and ADR 0005). Uploads the P2 daily file to S3 with zlib compression.

All intermediate and final NetCDFs are written to a temporary directory that is cleaned up after the run.

## Directory structure

```
oer/
├── app.py                              # Lambda handler (entry point)
├── oer/
│   ├── __init__.py
│   ├── oer.py                          # OerCorrection class: S3 I/O, pipeline orchestration
│   ├── compute_polygon_correction.py   # Spline fitting, correction evaluation, correction application
│   └── oerfit.py                       # Low-level cubic spline solver (La Traon & Ogor, 1998)
├── tests/
│   ├── __init__.py
│   ├── test_oer.py                     # Consistency, empty-input, and apply-correction tests
│   ├── test_oerfit.py                  # Spline solver input validation and output shape tests
│   └── sample_data/
│       ├── sample_inputs/              # 13 crossover files + 1 daily file (gzip-compressed)
│       └── sample_output/              # Reference polygon, correction, and daily file (gzip-compressed)
├── Dockerfile
├── requirements.txt
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

From the `oer/` directory:

```bash
source .venv/bin/activate  # or use the devcontainer
python -m unittest discover -s tests -t . -v
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

`test_oerfit.py`:

| Test class               | What it tests                                                           |
|--------------------------|-------------------------------------------------------------------------|
| `InputValidationTestCase`| Rejects mismatched ptime/dssh and dssh/trackid array sizes              |
| `OutputShapeTestCase`    | Coef has 4 rows, columns match intervals, tbrk sorted, RMS/nint shapes, nint total matches input |

Tolerances:
- Spline coefficients: `1e-9` absolute
- OER, SSH: `1e-10` absolute

## Dependencies

Key libraries (see `requirements.txt`):

- `xarray` / `netcdf4` / `h5netcdf` / `h5py` — reading and writing NetCDF files
- `numpy` / `scipy` — numerical computation and spline evaluation (`PPoly`)
- `boto3` / `s3fs` — AWS S3 access
- `dask` — lazy array backend for `xr.open_mfdataset`
