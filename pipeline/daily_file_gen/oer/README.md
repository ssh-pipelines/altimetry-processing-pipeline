# Orbit Error Reduction (OER)

Fits a cubic-spline polygon to self-crossover SSH differences and applies the resulting correction to a processing-level-1 daily file, producing a level-2 daily file with reduced orbit error.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## Usage

**Lambda event parameters:**

| Parameter | Example      | Description                     |
|-----------|--------------|---------------------------------|
| `date`    | `2025-01-01` | Processing day (ISO 8601)       |
| `source`  | `S6`         | Satellite source (`S6`, `GSFC`) |
| `bucket`  | `my-bucket`  | S3 bucket for input/output      |

**Inputs:**
- Crossover NetCDFs from `s3://{bucket}/crossovers/p1/{source}/{year}/`
- Daily file NetCDF from `s3://{bucket}/daily_files/p1/{source}/{year}/`

**Outputs:**
- Polygon NetCDF uploaded to `s3://{bucket}/oer/{source}/{year}/`
- Correction NetCDF uploaded to `s3://{bucket}/oer/{source}/{year}/`
- Level-2 daily file uploaded to `s3://{bucket}/daily_files/p2/{source}/{year}/`

## Project Structure

```
oer/
  app.py                              # Lambda handler (entry point)
  requirements.txt
  Dockerfile
  oer/
    oer.py                            # OerCorrection class: S3 I/O, pipeline orchestration
    compute_polygon_correction.py     # Spline fitting, correction evaluation, correction application
    oerfit.py                         # Low-level cubic spline solver
  tests/
    test_oer.py                       # Consistency, empty-input, and apply-correction tests
    test_oerfit.py                    # Spline solver input validation and output shape tests
    sample_data/
      sample_inputs/                  # 13 crossover files + 1 daily file (gzip-compressed)
      sample_output/                  # Reference polygon, correction, and daily file (gzip-compressed)
```

## How It Works

1. **`fetch_xovers()`** -- Streams crossover files from S3 for a window around the target date (10 days back + 1 day padding on each side).
2. **`create_polygon()`** -- Extracts crossover SSH differences, filters to the target day with a 2-hour margin, and fits a piecewise cubic spline via `oerfit`. Returns zero coefficients when no data falls within the window.
3. **`evaluate_correction()`** -- Evaluates the fitted spline at each daily-file time step to produce an additive OER correction.
4. **`apply_correction()`** -- Adds the OER correction to `ssha` and `ssha_smoothed`, attaches the `oer` variable, and sets `product_generation_step` to `"2"`.

## Development

```bash
cd pipeline/daily_file_gen/oer

# Create venv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run tests (34 total: 20 consistency, 5 empty-input, 3 apply-correction, 6 oerfit)
python -m unittest discover -s tests -t . -v
```

### Test Data

Sample input/output files in `tests/sample_data/` are gzip-compressed to reduce repo size. The test suite automatically decompresses them to a temp directory at runtime.

### Consistency Tests

`ConsistencyTestCase` processes the sample crossover and daily files through all three pipeline steps and compares every output field against known-good reference files. Tolerances:
- Spline coefficients: `1e-9` absolute
- OER, SSH: `1e-10` absolute
