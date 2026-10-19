# start_run

Opens a product-keyed pipeline run. The first state of `pipeline.asl.json`, and the
only one that runs exactly once before either tier. See
[ADR 0010](../../../docs/adr/0010-product-keyed-orchestration.md).

A trigger names a **Published product**, not a **Source**. Four facts the parent state
machine needs to act on that follow, none of which ASL can derive — it can read neither
the Source registry nor a clock:

| Returned | Why it is resolved here |
|---|---|
| `contributors` | The `enabled` Sources feeding the line, for the source-tier Map to fan out over. A thin wrapper over `utilities.products.contributing_sources`, not a second copy of the derivation rule |
| `run_id` | Minted once and handed to both tiers, so one run is traceable across `pipeline_runs/{source}/` and `pipeline_runs/{published_product}/`. pipeline_init minting its own was sufficient only while one run meant one Source |
| `product_jobs_key` | The product tier's manifest address. Computed here so `merge_manifests` needs no layout knowledge and can stay a self-contained zip target |
| `gridded` | Whether the line has a product tier at all. A single high-latitude satellite publishes along-track only, and the Choice that skips the gridded tier cannot consult `published_products:` itself. `utilities.products.publishes_simple_grid`, the same predicate `run_summary` uses to decide which tiers to reconcile |

It also writes the run's parameters sidecar, because the parent is where the invocation
parameters arrive. Two sidecars exist per run and answer different questions: this one
records how the run was *asked for*, each pipeline_init's records what the window
*resolved to* for its Source after capping to that Source's date range.

## Lambda input

```json
{
  "bucket": "my-bucket",
  "published_product": "NASA-SSH"
}
```

| Parameter           | Required | Description                                                      |
|---------------------|----------|------------------------------------------------------------------|
| `published_product` | yes      | The released line to run. A `source` key is the pre-Phase-4 shape and is rejected rather than honoured |
| `bucket`            | yes      | S3 bucket (or `BUCKET_NAME` env var)                              |
| `start`, `end`      | no       | ISO date range, echoed to every contributor's pipeline_init       |
| `force_update`      | no       | Echoed to every contributor's pipeline_init                      |

## Lambda output

```json
{
  "published_product": "NASA-SSH",
  "bucket": "my-bucket",
  "run_id": "20260219T120000",
  "contributors": ["GSFC", "S6"],
  "product_jobs_key": "pipeline_runs/NASA-SSH/20260219T120000/jobs.json",
  "gridded": true,
  "start": null,
  "end": null,
  "force_update": false
}
```

The envelope is threaded through both tiers. `start` / `end` / `force_update` are echoed
rather than dropped so the source-tier Map can hand them to each `pipeline_init`;
losing them here would silently turn a `force_update` backfill into a nominal run.

## Empty fan-out is an error, not a quiet run

Both ways the contributor list can come back empty raise, with distinct messages,
because the fixes differ:

- **no registered contributor** — the identity resolves to no Source at all, i.e. a typo
  in the trigger input;
- **registered but none `enabled`** — the line exists but nothing is in production.

Neither may return an empty list. A run that fans out over nothing succeeds having
produced nothing, which downstream is indistinguishable from a week with no new upstream
data. `enabled` defaults to `false` precisely so Sources can be staged in config, which
makes this the expected shape of a misconfiguration rather than an exotic one.
`utilities/tests/test_schedules.py` catches the same condition statically, at the moment
a schedule is added or an `enabled` flag is flipped.

## Why it is containerized

Like `run_summary` and unlike the other `pipeline/infra/` Lambdas, this one **ships
`utilities`** — it exists in order to read the Source registry, which a zip target does
not carry (ADR 0004: zip targets are self-contained). The target registry derives
`container` from the presence of a Dockerfile, so it registers automatically.

## IAM

- `s3:PutObject` — the run parameters sidecar

## Tests

Run `./scripts/test.sh start_run` from the repo root (after `uv sync --extra dev`).
Tests exercise the real Source registry rather than a stub: the point of the stage is
that it is the registry's one runtime reader, so a stubbed registry would pin the
wrapper without pinning the rule it wraps.
