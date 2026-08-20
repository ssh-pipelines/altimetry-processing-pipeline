# run_summary

Success-path reconciliation Lambda. See [ADR 0005](../../../docs/adr/0005-job-outcome-contract-and-run-summary.md)
and `RUN_SUMMARY_CONTRACT_PLAN.md`.

Runs once at the top of `pipeline.asl` (replacing `Notify Success`) after the gridded
pipeline succeeds. Given `{jobs_key, bucket, source}` it:

1. resolves both manifests (along-track = `jobs_key`; gridded = `sg_jobs_key(jobs_key)`),
   reading the **Job specs** (expected dates);
2. reads **Job outcomes** from each owned stage's `ResultWriter` SUCCEEDED files
   (`along_track ← {finalizer, unifier}`, `gridded ← {simple_grids, enso}`);
3. reads `pipeline_init`'s `run_params.json` sidecar (absent ⇒ `{}` ⇒ "scheduled defaults")
   so the notification can report how the run was invoked;
4. reads the diagnostic **bad_pass** stage's results (`results/bad_pass/` ResultWriter)
   and aggregates the flagged-pass counts per date;
5. reconciles expected vs produced per **Product pipeline**, collecting `missing`
   (and skip reasons) and a per-deliverable `provenance_incomplete` count;
6. writes the **Run summary** to `pipeline_runs/{source}/{run_id}/summary.json`;
7. renders and publishes the success SNS.

## Reading SUCCEEDED entries

Each owned stage's Map invoke task must unwrap the Lambda result with
`Output: {% $states.result.Payload %}`, so the `ResultWriter` persists the **Job outcome**
itself (not the raw Lambda invoke envelope). `_outcome_from_entry` also **defensively unwraps**
a `{"Payload": {…}}` envelope when the outcome's `status` key is absent — a regression guard
after the unifier Map originally omitted that unwrap and was reported as `produced: 0` despite
unification succeeding.

## Notification

The rendered email:

- shows invocation params (`start`/`end`/`force_update`, or "scheduled defaults");
- **folds the unifier's `nasa_ssh_p3` into the finalizer's `daily_file_p3` line** as a
  unification annotation (`16 produced → all unified to NASA-SSH`, or `→ 0 of 16 unified …`
  on a shortfall) — they are the same P3 file in two prefixes, so a separate row read as a
  phantom failure. `summary.json` still carries both keys;
- lists produced filenames (basenames) per deliverable, capped at 40 with an `… (N total)`
  overflow, alongside the `missing` dates and reasons;
- adds a **bad passes** diagnostic line *within* the `along_track` section (bad_pass runs
  inside the along-track chain, between xover_p2 and the finalizer) — the run's total flagged
  (cycle, pass) count and a per-date breakdown (`2026-02-01: 5`), or `none flagged` when the
  stage ran but flagged nothing. bad_pass is *not* a deliverable (it produces no P3, so there
  is nothing to reconcile against the manifest); the line is omitted entirely for runs with no
  bad_pass results. `summary.json` carries the aggregate under a top-level `bad_passes` key.

## Reading bad_pass results

Unlike the deliverable stages, the bad_pass Map's invoke task does **not** unwrap the Lambda
result (`Output: {% $states.result.Payload %}`), so its `ResultWriter` writes the raw Lambda
envelope wrapping the handler return `{date, source, count}`. `read_outcomes` /
`_outcome_from_entry`'s defensive envelope-unwrap (the same one that guards the deliverable
stages) yields the `{date, source, count}` payloads, which `summarize_bad_passes` then
aggregates. `count` is the number of passes flagged for that date.

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

Run `./scripts/test.sh run_summary` from the repo root (after `uv sync --extra dev`).
Reconciliation is unit tested with stubbed S3; `summarizer`'s pure functions need no AWS.
