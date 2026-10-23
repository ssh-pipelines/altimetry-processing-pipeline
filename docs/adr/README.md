# Architecture Decision Records

Each ADR records one decision that was not obvious at the time: the context that
forced it, what was chosen, and what was rejected. They are append-only — a
decision that gets reversed earns a new ADR that supersedes the old one rather
than an edit to it.

| # | Decision | Status |
|---|---|---|
| [0001](0001-pipeline-runtime.md) | `pipeline_runtime` is the shared scientific-stack contract: one base image pinning `numpy`/`xarray`/`netCDF4`/etc. so every stage that reads or writes a daily file agrees on the NetCDF encoding. | Accepted · 2026-05-11 |
| [0002](0002-aviso-l2p-mss-handling.md) | AVISO L2P skips the MSS diff-file swap. Bundle DTU's WGS84 DTU21 grid and bilinearly interpolate it per granule, because which MSS AVISO used is poorly documented. | Accepted · 2026-05-13 |
| [0003](0003-failure-surfacing.md) | Surface failures by routing parent state-machine `Catch` blocks into one shared `failure_handling` Lambda that classifies code / runtime / auth failures and publishes a single SNS notification per run. | Accepted · 2026-05-28 |
| [0004](0004-target-registry.md) | A target registry module is the single source of truth for build/deploy, with a change-impact query replacing `find` calls and hand-maintained shell arrays. | Accepted · 2026-06-04 |
| [0005](0005-job-outcome-contract-and-run-summary.md) | Reconcile the success path with a declared Job-outcome contract and a Run summary artifact, instead of inferring what a run produced by listing S3 and matching date tokens. | Accepted · 2026-06-08 |
| [0006](0006-reference-crossovers-against-nasa-ssh-p3.md) | High-latitude reference crossovers compare against the finalized, unified NASA-SSH **P3** — not a version-matched P1/P2 — which imposes a pipeline-ordering constraint. | Accepted · 2026-07-16 — addendum 2026-10-14: same bytes, now read from `products/` (0008) |
| [0007](0007-shared-basin-reference-files.md) | Consolidate basin reference files duplicated across stages into one shared directory. | Proposed · 2026-08-24 — loader and connection-table header implemented; directory move pending |
| [0008](0008-published-products-prefix.md) | Released artifacts live under a `products/` prefix in the same bucket, keyed on Published product; `daily_files/*` and friends are internal staging. A separate bucket was rejected because the pipeline consumes its own published product. | Accepted · 2026-10-14 |
| [0009](0009-publisher-stage-and-enabled.md) | The unifier generalizes into one `publisher` every source runs, family as an argument; the `unify` double gate is deleted rather than rekeyed because published identity is derivable, and `enabled` becomes the explicit production fact. | Accepted · 2026-10-14 |
| [0010](0010-product-keyed-orchestration.md) | A trigger names a Published product, not a Source. The parent composes two independently optional tiers — a source tier Mapped over contributors, a product tier for the gridded products — each on its own run path under one shared `run_id`. Event-driven fan-in was rejected because it destroys run-scoped reconciliation. | Accepted · 2026-10-15 |
| [0011](0011-count-based-smoother-gap-fill.md) | The smoother fills land gaps by counting omitted samples from each gap's length in local sample periods, replacing the integer-second grid. A uniform grid at one configured period was rejected because ERS-1 and Jason-1 clocks drift up to half a period within a pass. | Accepted · 2026-10-23 |

## Writing a new one

Number it sequentially, name the file `NNNN-kebab-case-title.md`, and open with a
`Status` / `Date` header. Keep the **Context** section long enough that a reader
who wasn't there can tell why the obvious alternative was wrong, and record the
options you rejected — that is the part that stops the decision being relitigated.
Add a row here.
