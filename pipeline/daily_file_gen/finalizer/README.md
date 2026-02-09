# Finalizer

Produces level-3 (P3) daily files from level-2 (P2) inputs. Applies a source-specific absolute offset to `ssha` and `ssha_smoothed`, flags bad passes using crossover-derived thresholds, sets metadata attributes, and uploads the finalized NetCDF to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## Usage

**Lambda event parameters:**

| Parameter | Example      | Description                     |
|-----------|--------------|---------------------------------|
| `date`    | `2025-01-01` | Processing day (ISO 8601)       |
| `source`  | `S6`         | Satellite source (`S6`, `GSFC`) |
| `bucket`  | `my-bucket`  | S3 bucket for input/output      |

Available sources are defined in `finalization/config/sources.yaml`. The handler validates the `source` parameter against this config at invocation time.

**Inputs:**
- P2 daily file from `s3://{bucket}/daily_files/p2/{source}/{year}/`
- Bad-pass JSON from `s3://{bucket}/aux_files/bad_passes/{source}/{date}.json` (optional; skipped if absent)

**Outputs:**
- P3 daily file uploaded to `s3://{bucket}/daily_files/p3/{year}/` (reference products, with `NASA` prefix) or `s3://{bucket}/daily_files/p3/{source}/{year}/` (high-latitude products)

## Project Structure

```
finalizer/
  app.py                                # Lambda handler (entry point)
  requirements.txt
  Dockerfile
  finalization/
    finalizer.py                        # Finalizer class: S3 I/O, offset, bad-pass flagging, metadata
    config/
      __init__.py
      sources.yaml                      # Per-source config (offset, date range, pass-flag thresholds)
      source_config.py                  # Dataclasses + YAML loader (lazy-cached)
  tests/
    __init__.py                         # Adds finalization/ to sys.path for test imports
    test_finalizer.py                   # Unit tests (config, source param, bad passes, process, attributes)
```

## How It Works

1. **`app.handler()`** -- Extracts `date`, `source`, and `bucket` from the event. Validates `source` against the YAML config. Constructs a `Finalizer` and calls `process()`.
2. **`Finalizer.__init__()`** -- Loads the source config, validates the processing date against the source's configured date range, and loads the bad-pass list from S3 (returns an empty DataFrame if no file exists).
3. **`process()`** -- Downloads the P2 daily file, then:
   - Writes pass-flag metadata attributes (`pass_flag_mean_num`, `pass_flag_rms_num`, etc.) from the source config.
   - Applies bad-pass flags: sets `nasa_flag = 1` for matching cycle/pass rows, NaNs `ssha_smoothed` for flagged observations, and records flagged passes in the `flagged_passes` attribute.
   - Handles the absolute offset: if the source offset is non-zero, removes any previously applied offset and adds the configured one. Records the value in `absolute_offset_applied`.
   - Sets `product_generation_step = "3"`, updates `history` and `granule_id`, and sorts global attributes alphabetically (case-insensitive).
   - Uploads the finalized file to S3 and removes the local temp copy.

## Source Configuration

Sources are defined in `finalization/config/sources.yaml` and loaded via dataclasses in `source_config.py`. Each source specifies:

| Field           | Description                                              |
|-----------------|----------------------------------------------------------|
| `product_type`  | `reference` or `high_latitude` (controls output path)    |
| `offset`        | Absolute offset applied to `ssha`/`ssha_smoothed` (m)    |
| `start_date`    | Earliest valid processing date for this source           |
| `end_date`      | Latest valid processing date (optional)                  |
| `pass_flag`     | Thresholds: `mean_num`, `rms_num`, `mean_threshold`, `rms_threshold` |

Current sources:

| Source | Product Type | Offset  | Start Date |
|--------|-------------|---------|------------|
| GSFC   | reference   | 0.0     | 1992-10-13 |
| S6     | reference   | 0.0291  | 2024-01-20 |

To add a new source, add an entry to `sources.yaml`. No code changes required.

## Development

```bash
cd pipeline/daily_file_gen/finalizer

# Create venv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run tests
python -m unittest discover -s tests -t . -v
```

### Tests

`test_finalizer.py` covers:

| Test class                | What it tests                                                    |
|---------------------------|------------------------------------------------------------------|
| `TestSourceConfig`        | YAML loading, GSFC/S6 config values, invalid source, available sources |
| `TestSourceParam`         | Source stored from event param, invalid source raises, date validation warns |
| `TestLoadBadPasses`       | Missing file returns empty DF, empty list, column rename, S3 key format |
| `TestGetDailyFile`        | Download when exists, raises when not found                      |
| `TestApplyBadPass`        | Flag matching cycle/pass, `flagged_passes` attr, `ssha_smoothed` NaN, no-match |
| `TestProcessGSFC`         | Upload path contains `NASA` and `p3`, offset is zero             |
| `TestProcessS6`           | Offset applied, previous offset removed before applying          |
| `TestProcessAttributes`   | `product_generation_step`, `history`, `granule_id`, pass-flag attrs, attribute sort |
| `TestProcessWithBadPasses`| Bad passes applied during `process()`                            |
