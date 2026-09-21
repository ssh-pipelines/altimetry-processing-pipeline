# Pipeline Init

Determines which dates need processing for a given satellite source by comparing NASA CMR granule modification times against existing daily files in S3. Writes a jobs manifest to S3 that all downstream along-track stages consume. Runs as an AWS Lambda function, invoked as the first step in the `along_track_pipeline.asl.json` Step Function.

## How it works

For each invocation, the Lambda:

1. **Validates the source** against `config/sources.yaml` and determines the date range — either from explicit `start`/`end` event parameters, or defaults to the source's `start_date` through the most recent processable date (based on a Monday cadence with a 10-day window buffer). Caps the end date at the source's `end_date` if configured.
2. **Queries existing daily files** in S3 by listing objects under the source's configured `s3_prefix` and extracting file dates via the `filename_pattern` regex. Records the `LastModified` timestamp for each date.
3. **Queries source modification times** depending on the source's `discovery_type`:
   - **CMR** (`cmr`): Queries NASA CMR for granule modification times using the source's configured `concept_id`(s). For sources with multiple collections (e.g., S6), resolves per-cycle/pass priority so the highest-priority collection wins.
   - **S3 bucket** (`s3_bucket`): Lists the source bucket and extracts modification times from S3 object metadata. If the source provides a `cycle_index_key`, reads a JSON index mapping cycle filenames to date ranges and uses the `LastModified` of covering cycle files as the source modification time for each date.
4. **Compares timestamps** — a date with granules needs processing if no daily file exists, or if the newest granule was modified after the daily file was last generated. With `force_update`, all dates are included unconditionally.
5. **Fills interior gaps** — some sources have dates with no upstream granules at all (GSFC 6.1 has one spanning 2019-02-24 through 2019-03-06). Those dates are planned with an empty `granules` list, which `daily_files` turns into an empty daily file carrying the expected structure and metadata.

   A granule-less date is only filled when it falls *before* the last date that has granules, i.e. when upstream coverage resumes after it. Granule-less dates at the trailing edge are skipped: upstream products land well behind real time, so their emptiness usually means "not published yet", and writing empty files there would churn the weekly simple grids and ENSO products until the data arrives. If nothing at all is enumerated for the range, nothing is planned — so **backfilling a known gap requires a range that extends past it**, far enough that real granules bound the gap on the late side.
6. **Writes the jobs manifest** to `s3://{bucket}/pipeline_runs/{source}/{run_id}/jobs.json` containing one entry per date that needs processing, plus a sibling `run_params.json` recording how the run was invoked (the given `start`/`end`/`force_update` overrides, a `defaults_used` flag, and the `resolved_start`/`resolved_end`). The params are a *separate* sidecar so `jobs.json` stays the bare list its iterators (Distributed Maps, `rewrite_manifest`, `set_sg_jobs`) depend on; `run_summary` reads the sidecar to report invocation parameters in the success notification (see ADR 0005).
7. **Returns** `jobs_key`, `bucket`, `source`, and `unify` — these fields are threaded through all downstream Step Function states.

## Directory structure

```
pipeline_init/
├── app.py                          # Lambda handler + CMR/S3 query logic
├── config/
│   ├── __init__.py
│   ├── sources.yaml                # Per-source config (satellite, S3 prefix, CMR collections)
│   └── source_config.py            # Dataclasses + YAML loader (lazy-cached)
├── Dockerfile
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

Each entry in the manifest is a job for one date, carrying the granule URIs that
`daily_files` should download. An empty `granules` list is meaningful, not a
defect: it instructs `daily_files` to write an empty daily file for a date the
source genuinely has no data for.

```json
[
  {"date": "2025-01-15", "source": "S6", "bucket": "my-bucket", "granules": ["s3://…/a.nc", "s3://…/b.nc"]},
  {"date": "2025-01-16", "source": "S6", "bucket": "my-bucket", "granules": []}
]
```

## S3 paths

| Path | Description |
|------|-------------|
| `{s3_prefix}/{year}/{filename_pattern}` | Existing daily files queried for modification times (read) |
| `pipeline_runs/{source}/{run_id}/jobs.json` | Jobs manifest written for downstream stages (write) |
| `pipeline_runs/{source}/{run_id}/run_params.json` | Invocation params sidecar read by `run_summary` (write) |

The `s3_prefix` and `filename_pattern` are source-specific (see Source Configuration below).

## Source configuration

Each source has settings at two levels:

**Global registry** (`utilities/sources.yaml`) — shared fields inherited by all stages: `product_type`, `unify`, `start_date`, `end_date`.

**Stage-local config** (`config/sources.yaml`) — pipeline_init-specific fields merged with the global registry:

| Field              | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| `satellite`        | Satellite identifier                                               |
| `s3_prefix`        | S3 prefix for existing daily files (e.g., `daily_files/p3/S6`)     |
| `filename_pattern` | Filename pattern with `{date8}` placeholder (e.g., `S6_alt_ref_at_v1_1_{date8}.nc`) |
| `collections`      | List of CMR collection concept IDs with priority (lower = preferred) |
| `source_bucket`    | *(S3 bucket sources only)* S3 bucket containing source files |
| `source_prefix_pattern` | *(S3 bucket sources only)* S3 prefix pattern with `{source}`, `{year}` placeholders |
| `source_filename_pattern` | *(S3 bucket sources only)* Filename pattern with `{source}`, `{date8}` placeholders |
| `cycle_index_key`  | *(S3 bucket sources only, optional)* S3 key to a JSON file mapping cycle filenames to `{"start", "end"}` date ranges. When set, cycle file `LastModified` is used as the source mod time for dates within the cycle's coverage. |

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

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh pipeline_init
```

## Dependencies

Key libraries (see the `pipeline_init` extra in the root `pyproject.toml`):

- `python-cmr` — querying NASA CMR for granule metadata
- `boto3` / `s3fs` — S3 listing and manifest upload
- `pyyaml` — source config loading
