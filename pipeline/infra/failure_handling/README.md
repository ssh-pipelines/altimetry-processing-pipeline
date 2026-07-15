# failure_handling

Lambda invoked from parent state machines' `Catch` paths. Reads the failed Distributed Map's `ResultWriter` output from S3, classifies each failed item as **Code failure**, **Runtime failure**, or **Auth failure**, deduplicates by `(category, errorType, errorMessage)`, and publishes one SNS notification with the result. Falls back to the top-level `Cause` when no per-item output exists (e.g. `pipeline_init` failures, `ItemReader` failures).

This Lambda is **failure-only**. The success notification it used to emit (by inferring produced files from live S3 listings) now lives in the `run_summary` Lambda, which reconciles declared **Job outcomes** against the jobs manifest — see [ADR 0005](../../../docs/adr/0005-job-outcome-contract-and-run-summary.md).

See [`docs/adr/0003-failure-surfacing.md`](../../../docs/adr/0003-failure-surfacing.md) for the full design — topology, wiring, alternatives rejected.

## Environment

| Var | Required | Source |
|---|---|---|
| `SNS_TOPIC_ARN` | yes | Per-env Lambda configuration |
| `AWS_REGION` | yes | Auto-injected by Lambda |

## IAM

- `sns:Publish` scoped to `SNS_TOPIC_ARN`.
- `s3:ListBucket` and `s3:GetObject` on the pipeline bucket (read-only; `pipeline_runs/*` is sufficient).

## Input shape

Set by each parent-SM `Catch`'s `Output` expression:

```json
{
  "stage": "<stage name matching the ResultWriter prefix>",
  "source": "<source name>",
  "bucket": "<pipeline bucket>",
  "jobs_key": "pipeline_runs/<source>/<run_id>/jobs.json",
  "errorOutput": { "Error": "...", "Cause": "..." }
}
```

`jobs_key` is optional — stages that fail before the manifest is written (e.g. `pipeline_init`) omit it. The Lambda treats a missing `jobs_key` as `run_id = "unknown"` and falls back to the top-level `Cause` rather than listing S3.

## Tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh failure_handling
```
