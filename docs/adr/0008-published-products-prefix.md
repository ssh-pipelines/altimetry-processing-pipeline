# ADR 0008: Released artifacts live under a `products/` prefix in the pipeline bucket

- **Status**: Accepted
- **Date**: 2026-10-14

## Context

The bucket drew no line between working state and released artifacts. Everything
was keyed on **Source**, and `NASA-SSH` occupied a Source slot in
`daily_files/p3/` despite not being a source: `utilities/sources/NASA-SSH.yaml`
carried a `common:` block and no stage sections, so it was never planned or
processed. It existed only to feed an identity to `daily_file_key`, which
interpolates `profile.source` as a directory segment. The glossary already
conceded the point ("Not a real upstream source").

Nothing was colliding. `products.yaml` splits the filename family by
`product_type`, so `NASA-SSH_alt_ref_at_v1_1_*.nc` and
`S3B_alt_hilat_at_v1_1_*.nc` could not be confused. The problem was that
`daily_files/p3/` mixed per-source intermediates with one published composite,
and that gets fifteen times worse as the high-latitude satellites land — each of
which publishes its own along-track product, in place, with no publication step
at all. A product that is also an internal staging file has no address a consumer
can be pointed at and no address the pipeline can treat as scratch.

## Decision

Two areas, one bucket.

**Internal staging**, keyed on Source: `daily_files/{p1,p2,p3}/{source}/`,
`crossovers/`, `oer/`, `bad_passes/`, `simple_grids/`, `enso_grids/`, `maps/`,
`pipeline_runs/`. Working state, free to be deleted and regenerated from
upstream.

**Released artifacts**, keyed on **Published product**:

```
products/NASA-SSH/along_track/{year}/NASA-SSH_alt_ref_at_v1_1_{YYYYMMDD}.nc
products/NASA-SSH/simple_grid/{year}/NASA-SSH_alt_ref_simple_grid_v1_1_{YYYYMMDD}.nc
products/NASA-SSH/indicators/{...}
products/NASA-SSH_S3B/along_track/{year}/NASA-SSH_S3B_alt_hilat_at_v1_1_{YYYYMMDD}.nc
```

Product-first, because that is what an external consumer wants and because
listing ergonomics barely matter here: every reader reconstructs exact keys, and
the only stage that lists is `pipeline_init`'s P3 scan, which stays internal.

Note the asymmetry. Each high-latitude *satellite* publishes its own along-track
product, but there is at most **one** high-latitude *grid*, composed from all of
them plus the reference product — so `products/NASA-SSH_S3B/` holds along-track
only. That composite grid is deferred, and its prefix waits on its name.

`products/` asserts **release shape**, not PO.DAAC ingestion: final filename,
final layout, product-owned in-file identity — the artifact a consumer would
fetch. PO.DAAC *delivery* is a separate, later act, gated on the product's
metadata being final (an `id` of `TBD` is the visible marker that it is not),
never on the file's location. This matters because a high-latitude file has to
live somewhere from its first run, and has to be readable by `xover` and the
gridder, so it lives under `products/` with a placeholder DOI.

ENSO grids and the ortho/plate PNGs are website assets, not released products,
and stay in the internal layout.

Every key under `products/` comes from one `published_*_key()` family in
`utilities/pipeline_layout.py`.

## Consequences

The pipeline reads its own published product, in two places:
`simple_gridder/gridder.py` grids it, and `xover/crossover/loader.py` crosses
every high-latitude source against it (ADR 0006). Both now read from
`products/`. The published area is also read-write for exactly one stage:
indicators writes through rather than being promoted, because nothing outside
the stage reads them and `indicators.nc` is self-read as a cache, so the
published copy *is* the cache. An accepted wart, and the alternative — a staged
copy nobody reads plus a promotion step — buys nothing.

The reference along-track product has **no internal staging representation**. It
does not come from one source, so there is nothing to stage it under; it exists
only under `products/`.

Migration is a prefix copy with no renaming. The pre-change pseudo-source P3
filename (`daily_file_filename` on the `NASA-SSH` profile) is byte-identical to
what the published template produces, and the same holds for the reference grid.
Seeding is therefore three `aws s3 cp --recursive` runs, not a reprocessing pass.
The indicators copy is mandatory rather than tidiness: `load_cached_indicators`
treats a missing key as "starting fresh", so against an empty published prefix
the first run would compute two or three records, skip the 1993 normalization,
and write a valid-looking GMSL product missing thirty years to a PO.DAAC-facing
filename, from a successful run.

## Considered options

- **A separate products bucket.** The cleaner boundary, and rejected on cost.
  `bucket` is single-valued throughout — SM input to Job spec
  `{date, source, bucket, granules}` to every Lambda, with the read-only
  `source_bucket` at `source_profile.py` the only second-bucket precedent. Since
  the pipeline consumes its own published product in two stages, and the
  reference product has no internal representation to fall back on, a second
  bucket would mean threading a second bucket name into the publisher *and* two
  consuming stages, plus making `pipeline_layout` say which bucket each key
  belongs to. A prefix does not foreclose it: because every published key comes
  from one function family, the split later is that one module plus a config
  value.

- **Keep publishing into `daily_files/p3/{published product}/`** — i.e. leave the
  pseudo-source in place and just stop calling it a source. Rejected: it keeps a
  released artifact inside the staging area, so "delete and regenerate the
  staging area" stops being safe, and it leaves each high-latitude satellite's
  product indistinguishable from its own intermediate.

- **Source-first layout** (`products/along_track/{identity}/`). Rejected: a
  consumer wants one prefix per product line, and grouping by family splits a
  single product's artifacts across three top-level directories.

- **Dual-write during migration.** Rejected: two truths is what this change
  removes, and any date is reproducible from upstream via `force_update`.
