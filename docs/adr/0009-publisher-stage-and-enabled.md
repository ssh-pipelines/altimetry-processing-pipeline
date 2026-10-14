# ADR 0009: One publisher stage every source runs, and `enabled` as the production fact

- **Status**: Accepted
- **Date**: 2026-10-14

## Context

The `unifier` existed to assemble the reference-mission record, and only that.
It was gated by two independent switches that both had to agree: `common.unify:
true` in a source's YAML, which the top-level state machine's `Need
Unification?` Choice tested, and an `unifier:` section, whose absence made the
Lambda raise. High-latitude sources had no publication step at all — their P3
*was* the product, in place.

Three things made that untenable as the high-latitude satellites land.

The gate had nothing left to express. S6B's `unify: false` turned out to be
incidental rather than a deliberate exclusion, and no source needs per-source
publish suppression. Its real job — keeping a not-yet-integrated source out of
production — was never what it did, since it never protected the high-latitude
sources at all.

Publication is derivable. There is no source whose published identity departs
from its `product_type`: `reference` publishes to `NASA-SSH`, `high_latitude` to
`NASA-SSH_{source}`. Many-to-one and one-to-one fall out of the same rule.

And the gridded pipeline was bolted to the wrong scope. `pipeline.asl.json`'s
`Need Unification?` Choice fell through to `SG Execution` on its default branch,
so a non-unified source ran the *full* gridded pipeline. Enabling S3B would have
produced an ENSO index and a GMSL curve from one high-latitude satellite. Never
hit in production, because high latitude was never enabled — but the coupling
was already wrong.

## Decision

**The `unifier` generalizes into a `publisher` that every source runs**, taking
the product family as an argument so one Lambda serves both along-track and
simple-grid promotion. Its event is
`{date, bucket, family, publish_as, contributors}`; it resolves the internal
source key and the published destination through `pipeline_layout` and performs
the same byte-level `copy_object` it always did. It stays off `pipeline_runtime`
(ADR 0001) because it still touches no NetCDF.

**The publish gate is deleted, not rekeyed.** `common.unify` is gone from
`SourceCommon`, from `pipeline_init`'s return, from `at_pipeline.asl.json`, from
the `Need Unification?` Choice, and from the finalizer's Job outcome. Nothing
replaces it on the wire; `pipeline_init` now returns the derived
`published_product` instead, because ASL cannot read the source registry. The
`unifier:` stage section is gone with it — there is nothing to opt into.

**`enabled: bool = False` on `SourceCommon`** is now the one thing that decides
whether a source is in production. Declared, not derived: a source in the
registry is not thereby in a run.

**`published_products:` in `products.yaml`** declares published-product identity,
but *only* the facts no Source can supply: which **Product** a line's gridded
output belongs to, and whether indicators are produced. Along-track identity is
derived. The composite grid is why this section exists at all — it is named after
no source, so nothing supplies its product family, filename template, or
metadata.

## Consequences

`enabled` is required by the other two decisions, not bookkeeping. Today
enablement is implicit in *which EventBridge schedules exist*, which works while
triggers are per-source. Once a trigger names a published product it fans out to
every source resolving to that product — so without `enabled`, the first
product-keyed run would pull S6B in, backfill it from 2025-11-26, and publish it
onto the five weeks of dates that contend with GSFC's end on 2025-12-31. The
problem is specific to the many-to-one reference line, where fan-out replaces
the schedule as the enablement mechanism. Defaulting to `false` is also what
makes the fifteen high-latitude YAMLs safe to add ahead of their schedules.

Contested dates are guarded twice, at different moments. A registry test asserts
that every `enabled` `reference` source has a date range disjoint from every
other, so flipping `enabled: true` on S6B fails at review time. The publisher
refuses to publish a date claimed by more than one contributor, so if that test
is ever bypassed the run fails loudly instead of letting execution order decide
what ships. The publisher's interface is one-date-many-contributors precisely so
that resolving contention later replaces its body without reshaping the event or
the Job outcome.

The `Need Unification?` Choice is gone, so publication is unconditional in the
still-per-source top-level state machine. Until orchestration is product-keyed,
**do not set `enabled: true` on a high-latitude source**: its run would reach the
gridded pipeline, and the grid stage now fails loudly there (no published
identity for a single high-latitude satellite declares a `simple_grid`) rather
than producing a per-satellite ENSO index. Loud, but still a failed run.

The top-tier `Catch` wiring of [ADR 0003](0003-failure-surfacing.md) shifts with
the topology. Where that ADR describes one top-tier notifying Catch (Unifier
Execution), `pipeline.asl.json` now has two — the along-track promotion and
`rewrite_manifest`, which moved out of the stage's own state machine so the
publisher is purely "promote this family over this manifest". Both are leaf
invocations with no intermediate parent SM, so the top tier is still their parent
tier, which is the rule ADR 0003 set. `sg_pipeline` gains a third, for the grid
promotion, alongside its existing per-stage Catches.

`run_summary` gains a published deliverable per tier (`published_along_track`,
`published_simple_grid`), both unconditional. Since one Lambda serves two tiers,
each tier's promotion writes its Job outcomes under its own
`results/publisher_{family}/` prefix — the same shape as `xover_{df_version}`.
The along-track line folds into the `daily_file_p3` line as a publication
annotation, so a publication shortfall shows as "2 of 16 published" rather than
a separate, confusing row.

## Considered options

- **Rekey the publish gate instead of deleting it** — e.g. a `publish:` stage
  section as the opt-in marker. Rejected once S6B's `unify: false` was confirmed
  incidental: with no source needing per-source publish suppression, the gate had
  nothing left to express, and its real job belongs to `enabled`, which also
  covers the high-latitude sources the gate never protected.

- **`published_name` as a per-source field**, with a table mapping each source to
  its line. Rejected: no source's published identity departs from its
  `product_type`, so the field would be a hand-maintained restatement of a
  derivable fact, with the usual failure mode of the two disagreeing.

- **Declaring `along_track:` per published identity in `published_products:`** —
  one entry per high-latitude satellite. Rejected for the same reason: each entry
  would restate its own source's `product_type`. The registry carries only the
  non-derivable half.

- **Two publisher stages, one per family.** Rejected: two near-identical copy
  Lambdas, two `targets.yaml` rows, two deploy paths, two READMEs — and the
  second would be about twenty lines. The families differ in what they fan out
  over and in which tier invokes them, neither of which is a reason for a second
  artifact.

- **Publisher infers officialness from its contributor set**, by comparing
  against the set derived from the registry. Rejected: set-equality against a
  derived expectation fails silently in both directions, and a diverged set would
  route a real product into the development directory on a *successful* run —
  the "product that looks complete" failure ADR 0005 exists to prevent.
  Officialness is declared by `publish_as` instead.

- **A published-product registry declaring each product's *inputs*.** Tempting,
  since gridded products are composites no single source can name. Rejected: the
  contributors are derivable from the source registry, so a declaration file
  would restate what the registry already knows and add a second place to keep in
  sync. The *identity* half is a different question and is declared — revisit the
  inputs half only if a grid needs a contributor set that source config cannot
  express.
