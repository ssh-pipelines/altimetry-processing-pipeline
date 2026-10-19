# ADR 0010: Product-keyed orchestration and the source/product tier split

- **Status**: Accepted
- **Date**: 2026-10-15

## Context

A trigger named a **Source**. One EventBridge schedule per source invoked
`pipeline.asl.json` with `{source: "S6"}`, and the whole execution — planning,
the along-track chain, publication, the gridded products, the run summary — was
keyed on that one source.

That shape stopped matching the products after [ADR 0008](0008-published-products-prefix.md)
and [ADR 0009](0009-publisher-stage-and-enabled.md). Two mismatches, pulling in
opposite directions.

**The reference line is many-to-one, and nothing expressed the many.** GSFC and S6
both publish to `products/NASA-SSH/`. With per-source triggers they ran as two
unrelated executions, each summarizing as though it were the whole product, each
rekeying *its own* manifest onto `NASA-SSH` for the gridded tier. On a handover
week that produced two gridded runs over the same published line, and two run
summaries each reporting half the dates it should have. The `rewrite_manifest`
Lambda was the visible symptom: "stamp a published product onto one source's
manifest" only makes sense if exactly one source feeds the product.

**The high-latitude line is one-to-one, and the gridded tier did not care.**
`pipeline.asl.json` ran the gridded pipeline on the default branch of its
`Need Unification?` Choice, so a high-latitude source reached simple grids, ENSO,
and indicators. ADR 0009 removed the Choice and made the grid stage fail there
instead of silently producing a per-satellite ENSO index — better, but still a
failed run on a correct configuration. Fifteen satellites are coming; the gridded
tier needs to be *unreachable* from their triggers, not merely fatal.

Both follow from one root cause named in the plan this implements: `sg_pipeline`
bundles two scopes. Simple grids are per-published-product; ENSO and indicators
are products of the reference line specifically. Neither is per-source.

A third mismatch was structural rather than behavioural. Because the gridded
manifest nested under the source's run path
(`pipeline_runs/S6/{run_id}/NASA-SSH/jobs.json`), every helper that derived a
results prefix had to take the manifest key and *ignore the identity it was
handed* — `stage_results_prefix` documented that it must use `parts[1]`,
`jobs_key_identity` carried the same caveat, and `summarizer` had a function whose
only job was detecting the five-segment form. Three pieces of code existed to
work around one address.

## Decision

**The trigger names a Published product.** `{published_product: "NASA-SSH"}`.
`pipeline.asl.json` becomes the composition of two independently optional tiers.

**A source tier, Mapped over the contributing sources.** Planning, the along-track
chain, and the promotion of each contributor's P3. One inline Map iteration per
contributor, each running `at_pipeline` and then the publisher.

**A product tier, keyed on the published product.** Simple grids, their promotion,
ENSO, indicators. Reached only when the line publishes a grid, which a single
high-latitude satellite does not — so ENSO and indicators are now unreachable from
a per-satellite trigger rather than fatal. `product_type` drives the fan-out, so
fifteen high-latitude satellites are fifteen schedules and no new code.

**A new `start_run` Lambda opens the run**, resolving everything the parent needs
that ASL cannot derive: the `enabled` contributors (a thin wrapper over
`contributing_sources`, not a second implementation of the rule), the one `run_id`
both tiers share, the product tier's manifest address, and whether the line has a
product tier at all. It also writes the run's invocation-parameters sidecar, the
parent being where those parameters arrive.

**`rewrite_manifest` becomes `merge_manifests`**, unioning the contributors' dates
into the product-tier manifest. It **fails the run** when two contributors claim
the same date.

**Each tier owns its own run path**, `pipeline_runs/{identity}/{run_id}/`, sharing
the one `run_id`. The product tier is a sibling of its contributors' paths, not a
child of one of them.

**The Run summary is per published product**, reconciling the union across every
contributor on the source tier against the one product-tier manifest. Its
top-level `source` is replaced by `published_product` + `contributors`.

**The schedules become tracked config** in `utilities/schedules.yaml` — a mirror of
hand-maintained AWS state, not a deployment source.

## Consequences

**A contributor's failure fails the whole product run.** `ToleratedFailureCount: 0`
on the source-tier Map. This is a real behaviour change: per-source executions were
independent, so a GSFC failure could not block S6. The alternative — tolerate and
continue — would run the gridded tier over whatever dates landed and publish a grid
across a hole, on a *successful* run, which is the failure mode ADR 0005 exists to
prevent. It costs little in practice: the reference contributors have disjoint date
ranges, so on any given week only one of them has data to contribute.

**Two empty fan-outs are now loud.** `start_run` raises when a published product
resolves to no `enabled` contributor, with distinct messages for "no registered
source resolves to this" (a typo) and "registered but none enabled" (a flag).
Previously, enablement was implicit in which schedules existed, so this state could
not arise; under fan-out it is the normal shape of a misconfiguration, and a run
over zero contributors succeeds having produced nothing — indistinguishable
downstream from a week with no new upstream data. `merge_manifests` raises on a
contested date for the same class of reason.

**Contested dates are now guarded three times**, at three moments: the registry
disjointness test at review time (ADR 0009), `merge_manifests` when the product
manifest is assembled, and the publisher when a date is promoted (ADR 0009). The
first two are new to this change. The publisher's guard remains the backstop for a
hand-invoked promotion that bypasses the parent entirely.

**§4.3's three workarounds are gone.** `stage_results_prefix` takes
`(identity, run_id, stage)` and holds no parsing; `jobs_key_identity` loses its
caveat and is simply the inverse of `jobs_manifest_key`; `summarizer`'s
five-segment detector is deleted. The leaf ASLs' `$p[0]/$p[1]/$p[2]` JSONata is
untouched — those three segments now *are* `pipeline_runs/{identity}/{run_id}` for
both tiers, which is what made the workarounds unnecessary rather than merely
relocated.

**The Run summary's wire shape changes, and only its own.** It gains its own
`RUN_SUMMARY_SCHEMA_VERSION = 2` rather than bumping the `SCHEMA_VERSION` in
`utilities/job_outcome.py`, which labels Job outcomes — a shape this change does not
touch. Conflating two independently-evolving wire formats under one number would
have relabelled every unchanged Job outcome.

**`pipeline_init` no longer mints the `run_id`**; it is a required parameter.
Defaulting it would put a contributor's leg under a run path nothing else in the
execution writes to, and `merge_manifests` would read a manifest address that does
not exist — a missing contributor rather than an error. Two params sidecars now
exist per run: `start_run`'s records how the run was *invoked* and is what the Run
summary reports, while each `pipeline_init`'s records what the window *resolved to*
for its source after capping, which differs per contributor and has no single value
for a multi-contributor run.

**`start_run` is containerized, not a zip infra Lambda.** The plan called for a zip;
a zip target is self-contained by construction (ADR 0004) and does not ship
`utilities`, which is precisely what this Lambda exists to read. It joins
`run_summary` as the second orchestration Lambda carrying the shared package.
`merge_manifests` stays a zip *because* `start_run` computes the addresses: it is
handed every key it reads and the one key it writes, so it holds no layout
conventions of its own.

**The source tier's Map is INLINE, not DISTRIBUTED.** Forced, not stylistic: a
distributed Map's output is a run summary, and the item results are the input to the
manifest merge. Contributor counts are small (one to a few), so the inline
concurrency ceiling is not a constraint. The mode also fixes the failure semantics:
inline has no partial-failure tolerance to set, so one failed contributor fails the
tier, and `ToleratedFailureCount` / `Label` are rejected outright rather than
ignored — a definition carrying them does not deploy at all.

**The ADR 0003 `Catch` topology shifts again.** The along-track promotion's notifying
Catch moves *inside* the source-tier Map, since that is where the publisher is now
invoked; `at_pipeline` still notifies its own failures, so its Catch inside the Map
goes straight to a Fail. The parent gains notifying Catches for `start_run` and
`merge_manifests`, both leaf invocations whose parent tier is the top tier — the rule
that ADR set.

**A third shape is designed for and deferred.** Product-tier-only, for the composite
high-latitude grid. The tiers being independently optional is what makes it
expressible, and it is also what avoids a fifteen-way fan-in barrier: the composite
depends on its contributors being current, but they have disjoint operating spans and
separate schedules, so a barrier would mostly wait on sources contributing nothing to
the window. Readiness by existence instead, as [ADR 0006](0006-reference-crossovers-against-nasa-ssh-p3.md)
already relies on.

**Development gridding is not a third shape.** The product tier already takes a
manifest, so a development grid run is the same state machine with a hand-written
`sg_jobs.json`, explicit contributors, and `publish_as: null`.

## Considered options

- **Event-driven fan-in.** The publisher emits a per-date event; an aggregator kicks
  the gridded pipeline for newly-published dates. Decouples cleanly and handles any
  number of contributors without a barrier. Rejected because it destroys run-scoped
  reconciliation, which is the premise of [ADR 0005](0005-job-outcome-contract-and-run-summary.md):
  there is no longer a *run* whose expected set can be compared against its produced
  set, only a stream. The Run summary is how this pipeline distinguishes "nothing to
  do" from "silently produced nothing", and that distinction is worth more than the
  coupling it costs.

- **Tolerate a contributor's failure and grid what landed.** Preserves the per-source
  independence the old topology had for free. Rejected: it publishes a grid over a
  partial window from a successful run. See Consequences.

- **Keep the gridded manifest nested under the source run path.** Would have left
  `rewrite_manifest` as a merge writing to `pipeline_runs/{first_contributor}/...`,
  which has no principled answer for *which* contributor, and would have kept all
  three of the workarounds above. Rejected: the nesting existed only because the
  gridded tier had no identity of its own, and it now has one.

- **Mint the `run_id` in ASL** from `$states.context.Execution.StartTime`. Avoids a
  Lambda. Rejected: reshaping an ISO-8601 timestamp into the `%Y%m%dT%H%M%S`
  convention needs several chained JSONata string functions, and nothing in the test
  suite evaluates JSONata — so it would be verifiable only by deploying. `start_run`
  has to exist for the contributor resolution regardless.

- **Keep minting the `run_id` per contributor in `pipeline_init`.** Rejected: the two
  tiers would have no shared identity, so the product tier could not address its
  contributors' manifests and a run would not be traceable end to end.

- **A separate Lambda for contributor resolution and for run identity.** Two targets,
  two deploy paths, two ASL states for what is one once-per-run step. Rejected; the
  single `start_run` is named for what it does rather than for the one job the plan
  anticipated.

- **Capture the schedules as a markdown table in `docs/`.** Readable, but nothing can
  pair it against the source registry, so it drifts silently — which is the failure the
  tracking is meant to prevent. `utilities/schedules.yaml` sits beside `targets.yaml`
  and `sources/`, and a test asserts both directions: every schedule names a resolvable
  published product with an enabled contributor, and every enabled Source's line has a
  schedule.
