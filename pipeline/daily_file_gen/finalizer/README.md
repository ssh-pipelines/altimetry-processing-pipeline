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
8. **Appends a `processing_history` step** (generation step 3) recording the bad-pass/offset application — see [`utilities/provenance.py`](../../../utilities/provenance.py) and ADR 0005.
9. **Uploads** the finalized P3 file to S3 and removes the local temp copy.

As a **deliverable stage** the handler returns a **Job outcome** (`utilities.job_outcome.JobOutcome`) declaring the P3 key it wrote — the success-side analog of the structured failure entry (ADR 0005). The Distributed Map's `ResultWriter` persists it to `SUCCEEDED_n.json`, and the `run_summary` Lambda reconciles it against the jobs manifest. The outcome's `metadata` carries the file's `processing_history`, a `provenance_complete` flag, and the `product_type`/`unify` source-config fields. An upload failure now **raises** (previously returned silently) so the failure path records it.

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

A **Job outcome** (`JobOutcome.to_dict()`):

```json
{
  "schema_version": 1,
  "stage": "finalizer",
  "status": "success",
  "date": "2025-01-15",
  "source": "S6",
  "outputs": [
    {"key": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250115.nc", "kind": "daily_file_p3"}
  ],
  "metadata": {
    "processing_history": [ ... ],
    "provenance_complete": true,
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

Defined in `state_machines/finalizer.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `finalizer` Lambda for each date. The Map's invoke task unwraps the Lambda result (`Output: {% $states.result.Payload %}`) so the **Job outcome** — not the raw Lambda envelope — is what the `ResultWriter` persists under `pipeline_runs/{source}/{run_id}/results/finalizer/` for `run_summary` to read.

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
