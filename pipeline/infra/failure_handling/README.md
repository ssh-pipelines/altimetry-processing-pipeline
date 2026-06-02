# failure_handling

Lambda invoked from parent state machines' `Catch` paths. Reads the failed Distributed Map's `ResultWriter` output from S3, classifies each failed item as **Code failure**, **Runtime failure**, or **Auth failure**, deduplicates by `(category, errorType, errorMessage)`, and publishes one SNS notification with the result. Falls back to the top-level `Cause` when no per-item output exists (e.g. `pipeline_init` failures, `ItemReader` failures).

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

## Tests

```bash
cd pipeline/infra/failure_handling
python -m unittest discover -s tests -t . -v
```
