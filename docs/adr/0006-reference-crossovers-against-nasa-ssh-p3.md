# Reference-mission crossovers compare against finalized NASA-SSH P3

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
