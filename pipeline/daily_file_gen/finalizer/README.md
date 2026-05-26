# Finalizer

Produces level-3 (P3) daily files from level-2 (P2) inputs. Applies a source-specific absolute offset to `ssha` and `ssha_smoothed`, flags bad passes using crossover-derived thresholds, sets metadata attributes, and uploads the finalized NetCDF to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

For each processing date, the Lambda:

1. **Validates the source** against `finalization/config/sources.yaml` and checks the processing date falls within the source's configured date range.
2. **Loads bad passes** from `s3://{bucket}/bad_passes/{source}/{date}.json` (optional; returns an empty DataFrame if absent).
3. **Downloads the P2 daily file** from `s3://{bucket}/daily_files/p2/{source}/{year}/`.
4. **Writes pass-flag metadata** — `pass_flag_mean_num`, `pass_flag_rms_num`, `pass_flag_mean_threshold`, `pass_flag_rms_threshold`, and `pass_flag_notes` from the source config.
5. **Applies bad-pass flags** — sets `nasa_flag = 1` for matching cycle/pass rows, NaNs `ssha_smoothed` for flagged observations, and records flagged passes in the `flagged_passes` attribute.
6. **Handles the absolute offset** — if the source offset is non-zero, removes any previously applied offset (via `absolute_offset_applied` attribute) and adds the configured one to both `ssha` and `ssha_smoothed`.
7. **Sets global attributes** — `product_generation_step = "3"`, `history`, `granule_id`, `absolute_offset_applied`, and sorts all global attributes alphabetically (case-insensitive).
8. **Uploads** the finalized P3 file to S3 and removes the local temp copy.

On success, the handler returns `product_type` and `unify` from the source config alongside the original event fields, which downstream steps (e.g., the unifier) use to decide further processing.

## Directory structure

```
finalizer/
├── app.py                              # Lambda handler (entry point)
├── finalization/
│   ├── finalizer.py                    # Finalizer class + apply_bad_pass()
│   └── config/
│       ├── __init__.py
│       ├── sources.yaml                # Per-source config (offset, pass-flag thresholds)
│       └── source_config.py            # Dataclasses + YAML loader (lazy-cached)
├── tests/
│   ├── __init__.py                     # Adds finalization/ to sys.path for test imports
│   └── test_finalizer.py              # Unit tests
├── Dockerfile
├── requirements.txt
└── README.md
```

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "source": "S6"
}
```

All three fields are required. Available sources are defined in `finalization/config/sources.yaml`.

## Lambda output

```json
{
  "status": "success",
  "data": {
    "bucket": "my-bucket",
    "date": "2025-01-15",
    "source": "S6",
    "product_type": "reference",
    "unify": true
  }
}
```

## S3 paths

| Path | Description |
|------|-------------|
| `daily_files/p2/{source}/{year}/{prefix}_{YYYYMMDD}.nc` | Input P2 daily file (read) |
| `bad_passes/{source}/{date}.json` | Bad-pass list from upstream stage (read, optional) |
| `daily_files/p3/{source}/{year}/{prefix}_{YYYYMMDD}.nc` | Output P3 daily file (write) |

Filename prefix is determined by the global source registry (`utilities/source_profile.py`): `{source}_alt_ref_at_v1_1` for reference products, `{source}_alt_hilat_at_v1_1` for high-latitude products.

## Source configuration

Each source has settings at two levels:

**Global registry** (`utilities/sources.yaml`) — shared fields inherited by all stages: `product_type`, `unify`, `start_date`, `end_date`.

**Stage-local config** (`finalization/config/sources.yaml`) — finalizer-specific fields merged with the global registry:

| Field       | Description                                              |
|-------------|----------------------------------------------------------|
| `offset`    | Absolute offset applied to `ssha`/`ssha_smoothed` (m)    |
| `pass_flag` | Thresholds: `mean_num`, `rms_num`, `mean_threshold`, `rms_threshold` |

Current sources:

| Source | Product Type | Offset  | Start Date | End Date   |
|--------|-------------|---------|------------|------------|
| GSFC   | reference   | 0.0     | 1992-10-25 | 2024-01-20 |
| S6     | reference   | 0.0291  | 2024-01-21 |            |
| S6B    | reference   | 0.0     | 2025-11-26 |            |

To add a new source, add an entry to `utilities/sources.yaml` first, then add the finalizer-specific entry to `finalization/config/sources.yaml`. No code changes required.

## Step Function

Defined in `state_machines/finalizer.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `finalizer` Lambda for each date. Results are written to `pipeline_runs/results/finalizer/` in S3.

## Running tests

From the `finalizer/` directory:

```bash
source .venv/bin/activate  # or use the devcontainer
python -m unittest discover -s tests -t . -v
```

### Test coverage

| Test class                | What it tests                                                    |
|---------------------------|------------------------------------------------------------------|
| `TestSourceConfig`        | YAML loading, GSFC/S6 config values, invalid source, available sources |
| `TestSourceParam`         | Source stored from event param, invalid source raises, date validation warns |
| `TestLoadBadPasses`       | Missing file returns empty DF, empty list, column rename, S3 key format |
| `TestGetDailyFile`        | Download when exists, raises when not found                      |
| `TestApplyBadPass`        | Flag matching cycle/pass, `flagged_passes` attr, `ssha_smoothed` NaN, no-match |
| `TestProcessGSFC`         | Per-source upload path, offset is zero                           |
| `TestProcessS6`           | Offset applied, previous offset removed before applying          |
| `TestProcessAttributes`   | `product_generation_step`, `history`, `granule_id`, pass-flag attrs, attribute sort |
| `TestProcessWithBadPasses`| Bad passes applied during `process()`                            |
| `TestProcessS6B`          | Per-source upload path, granule ID uses source name              |

## Dependencies

Key libraries (see `requirements.txt`):

- `netCDF4` / `h5netcdf` / `h5py` — reading and writing daily file NetCDFs
- `numpy` / `pandas` — numerical computation and bad-pass DataFrames
- `boto3` / `s3fs` — AWS S3 access
- `pyyaml` — source config loading
