# ADR 0005: Success-path reconciliation via a Job-outcome contract and a Run summary artifact

- **Status**: Accepted
- **Date**: 2026-06-08

## Context

ADR 0003 chartered `failure_handling` for **failures**: it reads the Distributed Map's `ResultWriter` `FAILED_n.json` from S3, classifies, and publishes one SNS notification per failed run. The **success** notification was added later and bolted onto the same Lambda (`lambda_handler` branches on `event.get("success")`). That success path does not read a contract — it *infers* outcomes:

- it lists live S3 objects under the product prefixes and string-matches the `YYYYMMDD` token in filenames to decide what a run produced;
- it duplicates the `results/{stage}/` prefix convention and (for filenames) the product-layout conventions, because `failure_handling` is an infra/zip Lambda that deliberately ships no `utilities` and therefore cannot import `pipeline_layout`.

This is the "jammed in" smell: the notifier reverse-engineers layout it isn't allowed to own. It also cannot answer the question operators actually have — *expected vs produced* — because that requires reconciling against the **manifest**, and the two **Product pipelines** have different manifests (`jobs.json` vs the Monday-remapped `sg_jobs.json`).

Two domain facts constrain any fix:

1. **Stages never pass payloads forward.** Each **stage** is an independent Distributed Map keyed off the static manifest; stages communicate only through S3 daily-file artifacts. So provenance cannot be threaded stage-to-stage through Step Functions — it must ride *in the file*.
2. **The single `jobs_key` reaching the success path already locates everything.** On success, `pipeline.asl` hands the notifier one `jobs_key` (`…/NASA-SSH/jobs.json` for unified runs, `…/jobs.json` otherwise). From it, both manifests and all produced outcomes are reachable: along-track expected = the key itself; gridded expected = `jobs_key.replace("/jobs.json", "/sg_jobs.json")` (the exact convention `set_sg_jobs` writes by); produced = `ResultWriter` `SUCCEEDED_n.json` under `…/{run_id}/results/{stage}/`.

## Decision

Replace inference with a declared contract, and split success out of `failure_handling`.

### 1. Job outcome contract

Every **deliverable stage** — `finalizer` (P3), `unifier` (NASA-SSH P3), `simple_grids`, `enso` — returns a **Job outcome** instead of a thin `{status, data}`:

```jsonc
{
  "schema_version": 1,
  "stage": "finalizer",
  "status": "success",            // success | skipped
  "date": "2025-01-07",
  "source": "S6",                 // NASA-SSH for the unifier's output
  "outputs": [                    // the stage DECLARES the keys it wrote
    { "key": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250107.nc", "kind": "daily_file_p3" }
  ],
  "metadata": { ... }             // provenance snapshot + stage extras
}
```

The producing stage already computed its output key via `pipeline_layout`; it now *reports* it. The Distributed Map's `ResultWriter` persists each return in `SUCCEEDED_n.json` — so a **Job outcome** is the **success-side analog of the structured `FAILED_n.json` entry** from ADR 0003. Same mechanism, same prefixes, opposite outcome. The schema lives in `utilities/` for producers; consumers read plain JSON. A `status: "skipped"` outcome with a `metadata.skip_reason` lets a stage explain an intentionally-absent deliverable (e.g. `simple_grids`/`enso` dropping a low-coverage date).

The four deliverable stages are the only ones changed; intermediate-artifact stages (`daily_files`, `oer`, `xover`, `bad_pass`) keep their current returns. The contract *shape* is universal, so extending emission to them later (for the deferred metadata overhaul) is additive.

### 2. `processing_history` provenance bus

Provenance rides **in the daily file's NetCDF global attributes**, the only carrier that survives the independent-Map topology. The file already accrues attributes (`source_files`, `product_generation_step`, `history`), but `history` is *overwritten* at each stage and is treated as an externally-constrained field. We add a pipeline-owned attribute, **`processing_history`**: a machine-readable, JSON-encoded list of step records that each file-writing stage (`daily_files`→P1, `oer`→P2, `finalizer`→P3) **appends** to (never overwrites). It survives to P3 and — because the **unifier** is a byte-level `s3.copy_object` — into the NASA-SSH product untouched. `finalizer` reads it back into its **Job outcome**'s `metadata` and derives `provenance_complete`. The **unifier** deliberately does *not* re-read it: it is a lightweight copy-only Lambda with no NetCDF engine (adding one to read a global attribute would violate its charter). The lineage is preserved *in* the copied file, and the along-track P3 (finalizer) outcome already carries the authoritative `provenance_complete` for that same byte-identical content; the `nasa_ssh_p3` outcome therefore omits the flag, which the **Run summary** reports as "unknown" rather than incomplete. The reprocessing-trigger work (deferred metadata overhaul) enters one field at planning time (`pipeline_init` knows *why* it reprocesses) and rides the same bus — additive, no new plumbing.

**Backward compatibility is a first-class concern:** every daily file already in S3 predates `processing_history`, and reprocessing an old date reads an upstream version that never carried it. Absence is therefore a *normal, expected* state — every reader (the append helper, the deliverable stages' read-back, and all downstream consumers) treats a missing attribute as `[]` and never errors. Critically, "legacy file, lineage not recorded" must be distinguishable from "lineage recorded and complete": because each step carries its `product_generation_step`, a consumer detects a *gap* (no step for an earlier generation level), and the deliverable stage derives a `provenance_complete: bool` into the **Job outcome** `metadata`, surfaced in the **Run summary**. Consumers — including the future metadata overhaul — branch on that flag rather than assuming lineage exists.

### 3. `run_summary` Lambda + Run summary artifact

A new lightweight, `utilities`-shipping Lambda (`run_summary`, modeled on `unifier`: `FROM lambda/python`, no `pipeline_runtime`, but bundles `utilities`) runs once at the top tier, replacing the `Notify Success` state in `pipeline.asl` (`SG Execution → Run Summary → Succeed`). It:

1. reconciles each **Product pipeline**'s **Job specs** (expected, from its manifest) against its **Job outcomes** (produced, from `ResultWriter`), using `pipeline_layout` for *all* key derivation — no string-munging;
2. writes one **Run summary** artifact, `pipeline_runs/{source}/{run_id}/summary.json`, containing a per-Product-pipeline section (expected count, per-deliverable produced **Outputs**, and `missing` items with reasons);
3. renders and publishes the **success** SNS notification.

The along-track Run summary claims the **unifier**'s outcomes via a stage→Product-pipeline ownership mapping (`along_track ← {finalizer, unifier}`), independent of the unifier's top-tier topology.

### 4. `failure_handling` reverts to failure-only

The `event.get("success")` branch and all success-path code (live-S3 listing, `YYYYMMDD` token matching, product-prefix construction) are deleted. `failure_handling` returns to its ADR-0003 charter: failures only. Success and failure become symmetric single-responsibility Lambdas (`run_summary` ↔ `failure_handling`).

## Consequences

**Positive:**
- The notifier stops inferring. Filenames are *declared* by the producer that wrote them; reconciliation is *computed* against the manifest, not guessed from S3 listings.
- Layout knowledge lives in exactly one place (`pipeline_layout`), used by a Lambda allowed to import it. The infra/zip notifier no longer duplicates conventions.
- Operators get *expected vs produced* with skip/missing reasons — impossible under the old inference.
- The `summary.json` artifact is a reusable substrate: the gallery website, audits, and the deferred metadata overhaul read and extend it rather than re-deriving run state.
- `processing_history` gives true `daily_files → … → unifier` lineage that the byte-copy preserves into NASA-SSH.

**Negative:**
- New moving parts: one Lambda, one ASL state, one artifact, plus IAM (`s3:GetObject`/`ListBucket` for manifests + ResultWriter, `s3:PutObject` for `summary.json`, `sns:Publish`).
- The four deliverable handlers' return shape becomes a load-bearing interface (`schema_version` guards evolution).
- `processing_history` writing touches three stages beyond the four that emit the contract (`daily_files`, `oer`).
- A `summary.json` is written only on success; a failed run produces no produced-side artifact (see Alternatives).
- Lineage is incomplete for any product that predates `processing_history` or is reprocessed from a pre-feature upstream version; the `provenance_complete` flag exposes this rather than back-filling it (the missing steps are unrecoverable). Consumers must honor the flag.

## Alternatives considered

- **Notifier computes the Run summary on the fly (no new Lambda).** Rejected: it leaves all manifest/prefix/layout conventions duplicated inside an infra Lambda that can't import `pipeline_layout` — relocating the smell, not removing it.
- **Orchestration passes explicit manifest keys + result prefixes to the notifier; notifier reconciles.** Rejected as the primary: addresses ADR 0003's noted prefix-duplication cleanup but still puts reconciliation logic in a `utilities`-less Lambda and produces no reusable artifact.
- **Sidecar provenance store (`…/provenance/{date}.json` each stage appends).** Rejected: a parallel bookkeeping artifact that can drift from the actual NetCDF; the file already carries provenance and the unifier copy preserves it.
- **One Lambda handling both success and failure (status quo).** Rejected: it is the source of the "jammed in" feeling and conflates two charters; ADR 0003 scoped `failure_handling` to failures.
- **Run summary on the failure path too (partial produced inventory).** Deferred: genuinely useful, and the `summary.json` shape already supports it, but wiring the summarizer into the failure tier touches the 12 Catches ADR 0003 flagged as load-bearing. Future extension.
- **Move the unifier into `at_pipeline` so the along-track Product pipeline owns it topologically.** Deferred: a separate ASL/topology refactor that revisits ADR 0003's "unifier has no parent SM" decision. The ownership mapping makes it unnecessary for this work.
