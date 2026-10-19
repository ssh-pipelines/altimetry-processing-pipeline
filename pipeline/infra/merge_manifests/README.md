# merge_manifests

The seam between the two tiers. Unions the contributors' source-tier manifests into one
product-tier manifest, and fails the run when two contributors claim the same date.
See [ADR 0010](../../../docs/adr/0010-product-keyed-orchestration.md).

The along-track manifests are keyed on a **Source**, the gridded manifest on the
**Published product** those Sources feed. Several Sources can contribute to one line and
the gridded output belongs to the line, so what the product tier needs is the set of
dates the *line* gained — not any one contributor's view of it.

Was `rewrite_manifest`, which stamped a published product onto one source's manifest and
nested the result under a `{published_product}/` segment. With a product-keyed parent
there are N manifests to combine rather than one to rekey, and the product tier has its
own run path, so there is nothing left to nest.

## Lambda input

```json
{
  "bucket": "my-bucket",
  "published_product": "NASA-SSH",
  "product_jobs_key": "pipeline_runs/NASA-SSH/20260219T120000/jobs.json",
  "source_results": [
    {"source": "GSFC", "jobs_key": "pipeline_runs/GSFC/20260219T120000/jobs.json"},
    {"source": "S6",   "jobs_key": "pipeline_runs/S6/20260219T120000/jobs.json"}
  ]
}
```

`source_results` is the source-tier Map's result array verbatim; only `source` and
`jobs_key` are read. Every key it touches is handed to it, including the one it writes —
it holds no layout conventions of its own, which is what keeps it a self-contained zip
target (ADR 0004). `start_run` owns the addresses.

## Lambda output

```json
{
  "jobs_key": "pipeline_runs/NASA-SSH/20260219T120000/jobs.json",
  "bucket": "my-bucket",
  "published_product": "NASA-SSH",
  "date_count": 23
}
```

`date_count` is what the parent's Choice gates the gridded tier on. The manifest is
written even when it is empty, so the Run summary reconciles against an explicit zero
rather than a missing object.

## Product-tier job specs

One per date, with `source` dropped rather than carried: a gridded job is not per-source,
and a stale `source` invites a downstream stage to key on the wrong thing. Granule URIs
are dropped too — the gridded tier reads published along-track files, never upstream
granules.

```json
[{"date": "2026-02-09", "bucket": "my-bucket", "published_product": "NASA-SSH"}]
```

## A contested date fails the run

The reference line's contributors are not guaranteed disjoint. GSFC ends 2025-12-31 and
S6 starts 2026-01-01 cleanly, but **S6B starts 2025-11-26**, overlapping GSFC by five
weeks — and under the derived-identity rule both resolve to the same published key.

Composing a contested date is a science decision that has not been made. Until it is,
the alternative to failing here is letting Map completion order pick which mission's data
ships under a PO.DAAC-facing filename. `test_enabled_reference_sources_have_disjoint_ranges`
in `utilities/tests/test_products.py` catches the same contention at review time, the
moment someone sets `enabled: true` on S6B; this is the runtime half, and it has to exist
because the registry test sees only declared date ranges, not what a hand-invoked window
actually planned.

A missing contributor manifest is likewise an error rather than an empty contribution:
every contributor in the fan-out ran `pipeline_init`, which writes a manifest even when it
plans nothing, so an absent one means the source tier did not get that far. Reading it as
"no dates" would grid a window with one contributor's data silently missing.

## IAM

- `s3:GetObject` — each contributor's manifest
- `s3:PutObject` — the product-tier manifest

## Tests

Run `./scripts/test.sh merge_manifests` from the repo root (after `uv sync --extra dev`).
S3 is stubbed in-memory; the Lambda touches nothing else.
