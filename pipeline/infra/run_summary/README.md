# run_summary

Success-path reconciliation Lambda. See [ADR 0005](../../../docs/adr/0005-job-outcome-contract-and-run-summary.md).

Runs once at the end of `pipeline.asl` (replacing `Notify Success`). Given the run
envelope `{bucket, published_product, run_id, contributors}` it:

1. resolves each **Product pipeline**'s manifest(s) and reads their **Job specs**
   (expected dates) — one per contributor on the source tier at
   `pipeline_runs/{source}/{run_id}/jobs.json`, one on the product tier at
   `pipeline_runs/{published_product}/{run_id}/sg_jobs.json`;
2. reads **Job outcomes** from each owned stage's `ResultWriter` SUCCEEDED files
   (`along_track ← {finalizer, publisher_along_track}`,
   `gridded ← {simple_grids, publisher_simple_grid, enso}`), likewise once per
   contributor on the source tier. One `publisher` Lambda serves both tiers, so each
   tier's promotion writes under its own `results/publisher_{family}/` prefix — the
   `stage` in `PRODUCT_PIPELINES` is that ResultWriter segment, not the Job outcome's
   own `stage` field;
3. reads `start_run`'s `run_params.json` sidecar from the **product** run path
   (absent ⇒ `{}` ⇒ "scheduled defaults") so the notification can report how the run
   was invoked;
4. reads the diagnostic **bad_pass** stage's results (`results/bad_pass/` ResultWriter,
   per contributor — bad_pass runs inside each along-track chain) and aggregates the
   flagged-pass counts per date;
5. reconciles expected vs produced per **Product pipeline**, collecting `missing`
   (and skip reasons) and a per-deliverable `provenance_incomplete` count;
6. writes the **Run summary** to `pipeline_runs/{published_product}/{run_id}/summary.json`;
7. renders and publishes the success SNS.

## A run is one Published product's, not one Source's

The along-track side reconciles the **union** across every contributor. A
`{published_product: "NASA-SSH"}` run fans out over GSFC and S6, each writing its own
manifest and its own ResultWriter outputs under its own leg of the run path, and a run
that produced all of GSFC's dates and all of S6's has no shortfall. Reconciling either
leg alone would report the other's dates as missing — on a clean run. The gridded side
has exactly one manifest, at the product run path. That asymmetry is the **Source tier**
/ **Product tier** split ([ADR 0010](../../../docs/adr/0010-product-keyed-orchestration.md)),
and it is why `summarizer` is handed `(published_product, run_id, contributors)` rather
than a single `jobs_key` to parse an identity out of.

`summary.json` carries `published_product` and `contributors` where it used to carry
`source`, under its own `schema_version: 2`. That version is deliberately *not*
`job_outcome.SCHEMA_VERSION` — Job outcomes did not change shape, and bumping the shared
constant would have relabelled every one of them.

A missing contributor manifest reconciles as empty rather than raising, unlike
`merge_manifests`, which fails on one. By the time the summary runs both tiers are done,
and erroring here would turn a reportable shortfall into no report at all.

## Reading SUCCEEDED entries

Each owned stage's Map invoke task must unwrap the Lambda result with
`Output: {% $states.result.Payload %}`, so the `ResultWriter` persists the **Job outcome**
itself (not the raw Lambda invoke envelope). `_outcome_from_entry` also **defensively unwraps**
a `{"Payload": {…}}` envelope when the outcome's `status` key is absent — a regression guard
after the promotion Map originally omitted that unwrap and was reported as `produced: 0` despite
the copies succeeding.

## Notification

The rendered email:

- heads with `Product: NASA-SSH ← GSFC, S6` — the product is the subject of the run, the
  contributors are how it was fed. A single-contributor line is still listed, so the
  reference line's two-mission handover weeks read the same as its one-mission weeks;
- shows invocation params (`start`/`end`/`force_update`, or "scheduled defaults");
- **folds the publisher's `published_along_track` into the finalizer's `daily_file_p3` line**
  as a publication annotation (`16 produced → all published to NASA-SSH`, or
  `→ 0 of 16 published …` on a shortfall) — they are the same P3 file staged and released, so
  a separate row read as a phantom failure. `summary.json` still carries both keys.
  Publication is unconditional, so a run that produced P3s but published nothing shows the
  shortfall rather than omitting the row;
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

Unlike most `infra/` Lambdas (zip, no `utilities`), `run_summary` **ships `utilities`**
so it can use `pipeline_layout` for *all* key derivation instead of duplicating layout
conventions (the smell ADR 0005 removes). It is therefore a containerized **orchestration**
Lambda — the first of its kind here; `start_run` is the second, for the same reason
applied to the Source registry. The target registry derives `container` from the presence
of a `Dockerfile`, so it registers automatically; `targets.yaml` only declares its
`deployable: true`, `heavy: false` facts. Its eventual home is the planned
`src/orchestration/` directory (see project memory).

## IAM

- `s3:GetObject` + `s3:ListBucket` — manifests and ResultWriter outputs
- `s3:PutObject` — `summary.json`
- `sns:Publish` — success notification (`SNS_TOPIC_ARN`)

## Tests

Run `./scripts/test.sh run_summary` from the repo root (after `uv sync --extra dev`).
Reconciliation is unit tested with stubbed S3; `summarizer`'s pure functions need no AWS.
