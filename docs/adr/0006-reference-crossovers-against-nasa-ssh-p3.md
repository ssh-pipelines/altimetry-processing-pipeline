# ADR 0006: Reference-mission crossovers compare against finalized NASA-SSH P3

- **Status**: Accepted
- **Date**: 2026-07-16

## Context

High-latitude sources (e.g. S3B) have no self-crossovers, so their SSHA is assessed
via reference-mission crossovers against the **reference mission** — the unified
NASA-SSH along-track product. We compare against the *finalized, OER-corrected,
unified* NASA-SSH **P3**, not a df_version-matched p1/p2, because the reference is the
established truth and NASA-SSH only exists post-unifier at P3.

## Consequences

A real pipeline-ordering constraint: reference-mission processing (through the
finalizer and unifier) must complete for the relevant date window before high-latitude
reference crossovers can run.

## Considered options

- **Match the high-lat df_version (p1/p2), symmetric with the self-crossover flow** —
  rejected: NASA-SSH p1/p2 are pre-finalization and may not exist; the reference must be
  the finalized baseline, not an intermediate lifecycle stage.

## Addendum (2026-10-14): the same bytes, a new address

The decision stands unchanged — the reference side is still the finalized,
OER-corrected, unified reference record — but where that record lives has moved.
It is now the **published** reference along-track product under
`products/NASA-SSH/along_track/`, not `daily_files/p3/NASA-SSH/`, and
`daily_files/p3/` holds only per-source internal staging (ADR 0008).

Two things follow:

- The phrase "NASA-SSH only exists post-unifier at P3" is now "the reference
  product has no internal staging representation at all" — it is composed from
  several missions, so `products/` is the only place it exists. The ordering
  constraint is therefore unchanged in substance and sharper in statement: the
  reference line must be *published* for the window, not merely finalized.
- `reference_version` is gone from the xover source config. There was never a
  version to choose — this ADR fixed it at P3 — and a published along-track file
  is finalized by definition, so the field could only ever have been wrong. The
  config now names a published product (`reference_product: NASA-SSH`), which is
  a different kind of thing from a Source and is spelled that way.
