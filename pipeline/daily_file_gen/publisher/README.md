# Publisher

Promotes an internal **Staging** artifact to its released address under
`products/`. Every source runs it — there is no opt-in marker and no publish
gate, because which **Published product** a source feeds is derived from its
`product_type` (see [ADR 0009](../../../docs/adr/0009-publisher-stage-and-enabled.md)).

One Lambda serves both product families, with `family` as an argument. Invoked
once per date via a Distributed Map in `state_machines/publisher.asl.json`, which
is itself invoked once per tier: the along-track tier from inside
`pipeline.asl.json`'s source-tier Map, so once per contributing source; the
gridded tier from `sg_pipeline.asl.json`, once per published line.

## How it works

For each processing date, the Lambda:

1. **Resolves two keys** from `utilities.pipeline_layout` — the internal source
   key and the published destination. No filenames are configured here.
2. **Copies** with `s3.copy_object`. No data transformation: this is a
   server-side byte copy, and the file's `processing_history` attribute rides
   along unchanged (the publisher never opens the NetCDF).
3. **Returns** a **Job outcome** declaring the published key. Because it never
   opens the file it omits `provenance_complete` — the byte-identical internal
   artifact's own outcome carries the authoritative flag, and `run_summary`
   reports absence as "unknown".

| `family` | reads | writes |
|---|---|---|
| `along_track` | `daily_files/p3/{contributor}/{year}/` | `products/{publish_as}/along_track/{year}/` |
| `simple_grid` | `simple_grids/{publish_as}/{year}/` | `products/{publish_as}/simple_grid/{year}/` |

The grid family needs no contributors: a staged grid is keyed on its own output
identity, so the contributor list that produced it is not needed to find it. The
staged copy is left in place — ENSO and indicators read it, so promotion is a
copy rather than a move.

## Lambda input

The Map's `ItemSelector` composes one event per manifest date:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "family": "along_track",
  "publish_as": "NASA-SSH",
  "contributors": ["S6"]
}
```

`bucket`, `date`, `family` and `publish_as` are required. `publish_as` is the
published identity, which `pipeline_init` derived and the state machine carried
through on the wire. `contributors` is required for `family: along_track` and
ignored for `family: simple_grid`.

**The interface is one-date-many-contributors on purpose**, even though today's
implementation copies from exactly one file. Once two reference missions can
claim the same date — S6B starts 2025-11-26, five weeks before GSFC ends — this
becomes a composer, and that change should replace the body without reshaping the
event or the Job outcome. Until then a date claimed by more than one contributor
**fails**, rather than letting execution order decide what ships.

That guard is now the innermost of three. `merge_manifests` fails first, when the
product-tier manifest is assembled from the contributors' manifests; a registry
test asserting that every `enabled` `reference` source has a date range disjoint
from every other fails earliest, at review time. This one remains the backstop for
a hand-invoked promotion that bypasses the parent entirely.

## Lambda output

A **Job outcome** (`JobOutcome.to_dict()`) declaring the published key:

```json
{
  "schema_version": 1,
  "stage": "publisher",
  "status": "success",
  "date": "2025-01-15",
  "source": "NASA-SSH",
  "outputs": [
    {"key": "products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250115.nc",
     "kind": "published_along_track"}
  ],
  "metadata": {
    "family": "along_track",
    "copied_from": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250115.nc",
    "contributors": ["S6"]
  }
}
```

`source` is the published identity, not the contributor — the outcome describes
what was released. The `kind` differs per family (`published_along_track` /
`published_simple_grid`) so `run_summary` reconciles the two tiers separately.

## Internal and published filenames differ

Deliberately. `products.yaml` declares two templates per publishable product —
`filename_template` taking `{source}`, `published_filename_template` taking
`{published_name}` — so a P3 file and a released file can never be mistaken for
each other:

| Product | internal | published |
|---|---|---|
| `along_track_reference` | `S6_alt_ref_at_v1_1_*.nc` | `NASA-SSH_alt_ref_at_v1_1_*.nc` |
| `along_track_high_latitude` | `S3B_alt_hilat_at_v1_1_*.nc` | `NASA-SSH_S3B_alt_hilat_at_v1_1_*.nc` |

The simple grid is the one case where the two coincide, because its staged
segment is already the published identity.

## Source configuration

The publisher reads no stage section and declares no stage-specific fields. It
needs only the contributing source's identity profile (`common`), to resolve that
source's P3 key. The former `unifier:` opt-in marker is gone: with publication
derived there is nothing to opt into.

Whether a source runs at all is `enabled` in its `common` section — the one
declared production fact.

## Step Function

`state_machines/publisher.asl.json` is a single Distributed Map (max concurrency
500). Its `ItemSelector` merges the per-date manifest item with the run-level
`family` and `published_product`; the invoke task unwraps the Lambda result
(`Output: {% $states.result.Payload %}`) so the **Job outcome** is what the
`ResultWriter` persists. *(This unwrap was missing in the predecessor stage, so
`run_summary` saw the raw Lambda envelope and reported `produced: 0` — see
ADR 0005 / the run_summary README.)*

The `ResultWriter` prefix carries the family —
`pipeline_runs/{source}/{run_id}/results/publisher_{family}/` — so the two
tiers' outcomes never share a prefix. Same shape as `xover_{df_version}`.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh publisher
```

### Test coverage

| Test class                           | What it tests                                                      |
|--------------------------------------|--------------------------------------------------------------------|
| `TestAlongTrackKeys`                 | Each contributor's P3 → its line's published address; internal and published names differ; no contributors raises; a contested date refuses rather than picking one |
| `TestSimpleGridKeys`                 | Staged grid → published grid; contributors are not needed; a line publishing no grid raises |
| `TestUnknownFamily`                  | An unrecognized family raises                                      |
| `TestPublisherHandler`               | Both families issue the right `copy_object`                        |
| `TestPublisherJobOutcome`            | Outcome shape, per-family `kind`, and the deliberate absence of `provenance_complete` |
| `TestPublisherRejectsIncompleteEvents` | Missing `family` / `publish_as` / `date` raise                    |

## Dependencies

Key libraries (see the `publisher` extra in the root `pyproject.toml`):

- `boto3` — S3 copy operations
- `pyyaml` — source config loading
