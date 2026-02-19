# Pipeline Init

Determines which dates need processing for a given satellite source by comparing NASA CMR granule modification times against existing daily files in S3. Writes a jobs manifest to S3 that all downstream along-track stages consume. Runs as an AWS Lambda function, invoked as the first step in the `along_track_pipeline.asl.json` Step Function.

## How it works

For each invocation, the Lambda:

1. **Validates the source** against `config/sources.yaml` and determines the date range — either from explicit `start`/`end` event parameters, or defaults to the source's `start_date` through the most recent processable date (based on a Monday cadence with a 10-day window buffer). Caps the end date at the source's `end_date` if configured.
2. **Queries existing daily files** in S3 by listing objects under the source's configured `s3_prefix` and extracting file dates via the `filename_pattern` regex. Records the `LastModified` timestamp for each date.
3. **Queries NASA CMR** for granule modification times using the source's configured `concept_id`(s). For sources with multiple collections (e.g., S6), resolves per-cycle/pass priority so the highest-priority collection wins.
4. **Compares timestamps** — a date needs processing if: no daily file exists, no CMR granule exists but no daily file either, or the CMR granule was modified after the daily file was last generated. With `force_update`, all dates are included unconditionally.
5. **Writes the jobs manifest** to `s3://{bucket}/pipeline_runs/{source}/{run_id}/jobs.json` containing one entry per date that needs processing.
6. **Returns** `jobs_key`, `bucket`, `source`, and `unify` — these fields are threaded through all downstream Step Function states.

## Directory structure

```
pipeline_init/
├── app.py                          # Lambda handler + CMR/S3 query logic
├── config/
│   ├── __init__.py
│   ├── sources.yaml                # Per-source config (satellite, S3 prefix, CMR collections)
│   └── source_config.py            # Dataclasses + YAML loader (lazy-cached)
├── Dockerfile
├── requirements.txt
└── README.md
```

## Lambda input

```json
{
  "bucket": "my-bucket",
  "source": "S6"
}
```

| Parameter      | Required | Description                                              |
|----------------|----------|----------------------------------------------------------|
| `bucket`       | yes      | S3 bucket for daily files and jobs manifest               |
| `source`       | yes      | Satellite source (must match `config/sources.yaml`)       |
| `start`        | no       | Start date (ISO 8601). Defaults to source's `start_date`  |
| `end`          | no       | End date (ISO 8601). Defaults to most recent processable date |
| `force_update` | no       | Skip modification time checks; regenerate all dates (`true`/`false`) |

## Lambda output

```json
{
  "jobs_key": "pipeline_runs/S6/20250219T120000/jobs.json",
  "bucket": "my-bucket",
  "source": "S6",
  "unify": true
}
```

The output is consumed directly by the next Step Function state (Daily File Execution) and threaded through all subsequent stages.

### Jobs manifest format

Each entry in the manifest is a job for one date:

```json
[
  {"date": "2025-01-15", "source": "S6", "bucket": "my-bucket"},
  {"date": "2025-01-16", "source": "S6", "bucket": "my-bucket"}
]
```

## S3 paths

| Path | Description |
|------|-------------|
| `{s3_prefix}/{year}/{filename_pattern}` | Existing daily files queried for modification times (read) |
| `pipeline_runs/{source}/{run_id}/jobs.json` | Jobs manifest written for downstream stages (write) |

The `s3_prefix` and `filename_pattern` are source-specific (see Source Configuration below).

## Source configuration

Each source has settings at two levels:

**Global registry** (`utilities/sources.yaml`) — shared fields inherited by all stages: `product_type`, `unify`, `start_date`, `end_date`.

**Stage-local config** (`config/sources.yaml`) — pipeline_init-specific fields merged with the global registry:

| Field              | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| `satellite`        | Satellite identifier                                               |
| `s3_prefix`        | S3 prefix for existing daily files (e.g., `daily_files/p3/S6`)     |
| `filename_pattern` | Filename pattern with `{date8}` placeholder (e.g., `S6_alt_ref_at_v1_{date8}.nc`) |
| `collections`      | List of CMR collection concept IDs with priority (lower = preferred) |

Current sources:

| Source | Satellite | S3 Prefix            | Collections |
|--------|-----------|----------------------|-------------|
| GSFC   | GSFC      | `daily_files/p3/GSFC`| 1           |
| S6     | S6        | `daily_files/p3/S6`  | 3 (priority-resolved) |
| S6B    | S6B       | `daily_files/p3/S6B` | 1           |

To add a new source, add an entry to `utilities/sources.yaml` first, then add the pipeline_init-specific entry to `config/sources.yaml`. No code changes required.

## Step Function

Invoked as the first state ("Init pipeline") in `state_machines/along_track_pipeline.asl.json`. Unlike the Distributed Map stages downstream, this is a single Lambda invocation — it runs once per pipeline execution and produces the jobs manifest that all subsequent stages consume:

```
Init pipeline (this Lambda)
  → Daily File → Xover (p1) → OER → Xover (p2) → Bad Pass → Finalizer
```

## Dependencies

Key libraries (see `requirements.txt`):

- `python-cmr` — querying NASA CMR for granule metadata
- `boto3` / `s3fs` — S3 listing and manifest upload
- `pyyaml` — source config loading
