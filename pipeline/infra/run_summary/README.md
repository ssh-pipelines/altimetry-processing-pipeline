# run_summary

Success-path reconciliation Lambda. See [ADR 0005](../../../docs/adr/0005-job-outcome-contract-and-run-summary.md)
and `RUN_SUMMARY_CONTRACT_PLAN.md`.

Runs once at the top of `pipeline.asl` (replacing `Notify Success`) after the gridded
pipeline succeeds. Given `{jobs_key, bucket, source}` it:

1. resolves both manifests (along-track = `jobs_key`; gridded = `sg_jobs_key(jobs_key)`),
   reading the **Job specs** (expected dates);
2. reads **Job outcomes** from each owned stage's `ResultWriter` SUCCEEDED files
   (`along_track ← {finalizer, unifier}`, `gridded ← {simple_grids, enso}`);
3. reconciles expected vs produced per **Product pipeline**, collecting `missing`
   (and skip reasons) and a per-deliverable `provenance_incomplete` count;
4. writes the **Run summary** to `pipeline_runs/{source}/{run_id}/summary.json`;
5. publishes the success SNS.

## Why it lives in `infra/` but is containerized

Unlike the other `infra/` Lambdas (zip, no `utilities`), `run_summary` **ships `utilities`**
so it can use `pipeline_layout` for *all* key derivation instead of duplicating layout
conventions (the smell ADR 0005 removes). It is therefore a containerized **orchestration**
Lambda — the first of its kind here. The target registry derives `container` from the
presence of a `Dockerfile`, so it registers automatically; `targets.yaml` only declares its
`deployable: true`, `heavy: false` facts. Its eventual home is the planned
`src/orchestration/` directory (see project memory).

## IAM

- `s3:GetObject` + `s3:ListBucket` — manifests and ResultWriter outputs
- `s3:PutObject` — `summary.json`
- `sns:Publish` — success notification (`SNS_TOPIC_ARN`)

## Tests

`python -m unittest discover -s tests` (from this directory's venv). Reconciliation is unit
tested with stubbed S3; `summarizer`'s pure functions need no AWS.
